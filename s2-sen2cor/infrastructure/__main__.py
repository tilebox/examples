import pulumi
import pulumi_azure as az
import pulumi_azuread as azuread
from tilebox_iac import azure

config = pulumi.Config()
name = f"s2-{pulumi.get_stack()}"
provider = az.Provider(
    "azure",
    subscription_id=pulumi.Config("azure").require("subscriptionId"),
    features=az.ProviderFeaturesArgs(storage=az.ProviderFeaturesStorageArgs(data_plane_available=False)),
)
options = pulumi.ResourceOptions(provider=provider)
current = az.core.get_client_config(opts=pulumi.InvokeOptions(provider=provider))
identity_provider = azuread.Provider("entra", tenant_id=current.tenant_id)
identity_options = pulumi.ResourceOptions(provider=identity_provider)
application = azuread.Application(
    f"{name}-worker",
    display_name=f"{name}-worker",
    owners=[current.object_id],
    opts=identity_options,
)
worker = azuread.ServicePrincipal(
    f"{name}-worker",
    client_id=application.client_id,
    owners=[current.object_id],
    opts=identity_options,
)
password = azuread.ApplicationPassword(
    f"{name}-worker",
    application_id=application.id,
    opts=identity_options,
)
group = az.core.ResourceGroup(name, location=config.get("location") or "westeurope", opts=options)
data = azure.BlobStorage(
    name,
    resource_group_name=group.name,
    location=group.location,
    container_name="previews",
    public_read=True,
    opts=pulumi.ResourceOptions(provider=provider, protect=True),
)
scenes = az.storage.Container(
    f"{name}-scenes",
    name="scenes",
    storage_account_id=data.account.id,
    container_access_type="private",
    opts=pulumi.ResourceOptions(parent=data),
)
worker_access = az.authorization.Assignment(
    f"{name}-worker",
    scope=pulumi.Output.concat(data.account.id, "/blobServices/default/containers/", scenes.name),
    role_definition_name="Storage Blob Data Contributor",
    principal_id=worker.object_id,
    skip_service_principal_aad_check=True,
    opts=options,
)
preview_access = az.authorization.Assignment(
    f"{name}-preview-worker",
    scope=data.container_resource_id,
    role_definition_name="Storage Blob Data Contributor",
    principal_id=worker.object_id,
    skip_service_principal_aad_check=True,
    opts=options,
)

# Register the account/container in Console after the first deployment.
# Azure validates the webhook while creating the Event Grid subscription.
endpoint = config.get("webhookEndpoint")
notifications = []
if endpoint:
    if not endpoint.startswith("https://"):
        raise ValueError("webhookEndpoint must be the HTTPS endpoint returned by Tilebox Console")
    notifications.append(
        az.eventgrid.EventSubscription(
            f"{name}-ndvi",
            scope=data.account.id,
            included_event_types=["Microsoft.Storage.BlobCreated"],
            event_delivery_schema="EventGridSchema",
            subject_filter={
                "subject_begins_with": pulumi.Output.concat(
                    "/blobServices/default/containers/",
                    scenes.name,
                    "/blobs/v1/l2a/",
                ),
                "subject_ends_with": ".ready",
                "case_sensitive": True,
            },
            webhook_endpoint={"url": endpoint},
            delivery_properties=[
                {
                    "header_name": "X-Tilebox-Webhook-Secret",
                    "type": "Static",
                    "value": config.require_secret("webhookSecret"),
                    "secret": True,
                }
            ],
            opts=options,
        )
    )

pulumi.export("storageAccount", data.account.name)
pulumi.export("storageAccountResourceId", data.account.id)
pulumi.export("container", scenes.name)
pulumi.export("previewContainer", data.container_name)
pulumi.export("accountUrl", data.account_url)
pulumi.export("location", group.location)
pulumi.export(
    "workerEnvironment",
    pulumi.Output.secret(
        pulumi.Output.concat(
            "AZURE_STORAGE_ACCOUNT=",
            data.account.name,
            "\nAZURE_STORAGE_CONTAINER=",
            scenes.name,
            "\nAZURE_PREVIEW_CONTAINER=",
            data.container_name,
            "\nAZURE_TENANT_ID=",
            current.tenant_id,
            "\nAZURE_CLIENT_ID=",
            application.client_id,
            "\nAZURE_CLIENT_SECRET=",
            password.value,
        )
    ),
)
