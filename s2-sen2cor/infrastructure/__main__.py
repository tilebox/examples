import pulumi
import pulumi_azure as az
from tilebox_iac import azure

config = pulumi.Config()
group = az.core.ResourceGroup("sen2cor", location=config.get("location") or "uaenorth")
storage = azure.BlobStorage(
    "sen2cor-results",
    resource_group_name=group.name,
    location=group.location,
    account_name=config.require("storageAccountName"),
    container_name="results",
    public_read=True,
    opts=pulumi.ResourceOptions(protect=True),
)

# Create storage and the private registry before building the worker image.
deploy_workers = config.get_bool("deployWorkers") is not False
if deploy_workers:
    registry = az.containerservice.Registry(
        "sen2cor",
        resource_group_name=group.name,
        location=group.location,
        sku="Basic",
        admin_enabled=False,
        anonymous_pull_enabled=False,
        role_assignment_mode="LegacyRegistryPermissions",
    )
    az.authorization.Assignment(
        "image-builder",
        principal_id=az.core.get_client_config().object_id,
        scope=registry.id,
        role_definition_name="AcrPush",
    )
    pulumi.export("registryName", registry.name)
    pulumi.export("registryLoginServer", registry.login_server)

runner_image = config.get("runnerImage")
if deploy_workers and not runner_image:
    pulumi.log.info("Build the image in registryName, then set runnerImage to its digest and run pulumi up again.")

# Storage-only deployment also supports on-prem workers.
if deploy_workers and runner_image:
    network = azure.Network("sen2cor", resource_group_name=group.name, location=group.location)
    vault_id = config.require("keyVaultId")
    environment = {
        name: azure.Secret(f"sen2cor-{key}", vault_id=vault_id, secret_data=config.require_secret(key))
        for name, key in {
            "TILEBOX_API_KEY": "tileboxApiKey",
            "CDSE_ACCESS_KEY": "cdseAccessKey",
            "CDSE_SECRET_KEY": "cdseSecretKey",
        }.items()
    }
    environment.update(
        {
            "TILEBOX_CLUSTER": config.require("tileboxCluster"),
            "RESULTS_DATASET": config.require("resultsDataset"),
            "RESULTS_STORAGE_URL": pulumi.Output.concat(storage.account_url, storage.container_name),
        }
    )
    cluster = azure.AutoScalingCluster(
        "sen2cor",
        resource_group_name=group.name,
        location=group.location,
        subnet_id=network.subnet_id,
        ssh_public_key=config.require("sshPublicKey"),
        instance_type="Standard_D8s_v5",
        root_volume_size_gb=256,
        min_replicas_config=1,
        max_replicas_config=1,
        spot=False,
        environment_variables=environment,
        runner_image=runner_image,
        container_registry_id=registry.id,
        container_registry_server=registry.login_server,
        blob_container_ids=[storage.container_resource_id],
    )
    pulumi.export("runnerIdentityClientId", cluster.identity.client_id)

pulumi.export("storageAccountUrl", storage.account_url)
pulumi.export("containerName", storage.container_name)
pulumi.export("containerResourceId", storage.container_resource_id)
