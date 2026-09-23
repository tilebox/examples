# Azure deployment with local Pulumi state

This program creates result storage and a private Azure Container Registry, then
deploys the Sen2Cor worker after you build its image. You do not need a registry
account, a public image, or a local Docker installation. Azure builds the image
from this example's Dockerfile and source files.

**Result blobs are publicly readable.** Anyone with a blob URL can download the
L2A files, NDVI, RGB image, and completion records without credentials. Anonymous
container listing and writes are disabled; workers still authenticate writes with
managed identity. Do not use this setup for confidential data. The image registry
remains private.

This is a temporary choice until Tilebox's Azure storage-client authentication is
available. It does not add Azure BYOK support or Console thumbnail previews. The
workflow and notebook use obstore for Azure access, separately from that client.

The worker defaults to one non-Spot
Standard_D8s_v5 worker (8 vCPU, 32 GiB RAM) with a 256-GiB OS disk. Both min and max
are one to avoid CPU-driven scale-in interrupting long scene processing. Expand
only after measuring memory, disk, throughput, and interruption behavior.

## Prerequisites

- Azure CLI and the open-source Pulumi CLI. No Pulumi Cloud account is needed.
- A subscription with the selected VM quota and permission to create resources
  and role assignments, and run ACR builds. Region defaults to `uaenorth`; verify
  VM and ACR Tasks availability. Registry storage and builds incur Azure charges.
- An existing **RBAC-enabled Key Vault**, reachable by the deployment machine and
  workers. The deployer must be able to write secrets and grant access to them.
- A Tilebox API key and workflow cluster from the [Tilebox Console](https://console.tilebox.com),
  and the custom dataset created in the [catalog setup](../README.md#set-up-the-catalog-and-submit-a-job).
- CDSE S3 credentials from the
  [Copernicus credentials manager](https://eodata-s3keysmanager.dataspace.copernicus.eu/).
  Sign in with your CDSE account, choose **Add Credentials**, and copy both keys.
  If you need an account, follow the [CDSE registration instructions](https://documentation.dataspace.copernicus.eu/Registration.html).
- A sibling checkout of `tilebox/tilebox-iac` containing its Azure implementation:

```text
parent/
  examples/s2-sen2cor/infrastructure/
  tilebox-iac/tilebox_iac/azure/
```

The path dependency uses that checkout. Until Azure support is
merged and released, use the changes from the Azure implementation thread, not an
older `main` without that provider. The checkout must also include managed-identity
ACR pulls (`container_registry_id` and `container_registry_server`). Record the
reviewed IaC commit used for each deployment. It must also support the explicit
`BlobStorage(public_read=True)` option. Run `uv sync` here after placing it
alongside `examples`.

## Configure a file backend

Run these commands from this directory on a machine with durable, private disk:

```bash
az login
az account set --subscription YOUR_SUBSCRIPTION_ID
export ARM_SUBSCRIPTION_ID="$(az account show --query id -o tsv)"
mkdir -p "$HOME/.local/state/pulumi/s2-sen2cor"
chmod 700 "$HOME/.local/state/pulumi/s2-sen2cor"
pulumi login "file://$HOME/.local/state/pulumi/s2-sen2cor"
pulumi stack init dev --secrets-provider passphrase
uv sync

pulumi config set location uaenorth
pulumi config set storageAccountName YOUR_UNIQUE_LOWERCASE_ACCOUNT_NAME
pulumi preview
```

Pulumi prompts for secret values and the encryption passphrase. For automation,
inject `PULUMI_CONFIG_PASSPHRASE` from your secret manager, not shell history or a
committed file. The local backend replaces Pulumi Cloud state, not Azure itself:
Azure credentials and Azure charges still apply.

Back up the **entire backend directory**, stack configuration, and passphrase
separately. State contains sensitive infrastructure metadata even when configured
secrets are encrypted. Losing state does not delete Azure resources or stop their
cost. Local state does not coordinate deployments across machines: designate one
operator and never run competing updates from copied backend directories. Do not
keep the sole copy of state in an ephemeral orb. Stack configs and `.pulumi` are
ignored by Git; no state or credentials should enter this repository.

## Create storage and build the image

With `runnerImage` unset, the first update creates storage and the private registry
without starting a VM. Review the preview, then create those resources:

```bash
pulumi up
REGISTRY="$(pulumi stack output registryName)"
REGISTRY_SERVER="$(pulumi stack output registryLoginServer)"
IMAGE_TAG="s2-sen2cor:$(date -u +%Y%m%dT%H%M%SZ)"

az acr build --registry "$REGISTRY" --image "$IMAGE_TAG" --platform linux/amd64 .. &&
DIGEST="$(az acr repository show --name "$REGISTRY" --image "$IMAGE_TAG" --query digest -o tsv)" &&
pulumi config set runnerImage "$REGISTRY_SERVER/s2-sen2cor@$DIGEST"
```

Run these commands from `infrastructure`. The build uploads the parent example
directory, using its `.dockerignore` to exclude credentials, results, and unrelated
files. Keep that allowlist in place. The Dockerfile checks the Sen2Cor download and
runs its help command during the build. Review ESA's distribution terms before
sharing the image outside your registry.

The program grants your deployment identity `AcrPush`; allow time for that grant
to propagate before building. Use the same Azure identity for Pulumi and the CLI.
The [ACR build guide](https://learn.microsoft.com/en-us/azure/container-registry/container-registry-quickstart-task-cli)
explains remote builds. If you already built the image locally, you can instead
use `az acr login`, tag it for the exported registry, and `docker push` it; use its
digest in `runnerImage` as above. Laptop-only execution needs neither ACR nor this
deployment; use the [local Docker instructions](../README.md#run-on-a-laptop).

## Configure credentials and start the worker

Get `tileboxApiKey` and your cluster slug from the
[Tilebox Console](https://console.tilebox.com). Use the full dataset slug from the
[catalog setup](../README.md#set-up-the-catalog-and-submit-a-job).
For `cdseAccessKey` and `cdseSecretKey`, use the access key and secret key created
in the [CDSE S3 credentials manager](https://eodata-s3keysmanager.dataspace.copernicus.eu/);
these are not your CDSE account password. See the [CDSE S3 access guide](https://documentation.dataspace.copernicus.eu/APIs/S3.html).

```bash
pulumi config set keyVaultId /subscriptions/.../resourceGroups/.../providers/Microsoft.KeyVault/vaults/...
pulumi config set sshPublicKey "$(cat ~/.ssh/id_ed25519.pub)"
pulumi config set tileboxCluster YOUR_CLUSTER_SLUG
pulumi config set resultsDataset YOUR_FULL_DATASET_SLUG
pulumi config set --secret tileboxApiKey
pulumi config set --secret cdseAccessKey
pulumi config set --secret cdseSecretKey
pulumi preview
# After reviewing the VM and access grants:
pulumi up
```

Workers authenticate to ACR with managed identity and an `AcrPull` grant. Registry
admin passwords and anonymous pulls are disabled. `runnerImage` uses a digest so
replacements run the same image. Rebuild and update the digest when code changes;
do not unset `runnerImage` on an existing stack, because that removes workers.

The worker's managed identity gets Storage Blob Data Contributor scoped to the results container and
read access to the three Key Vault secrets. The container uses identity for
Azure access, and the VM retrieves other credentials from Key Vault at startup. Secrets
are not embedded as plaintext in Azure custom data. The host environment file
and Docker access remain privileged. RBAC changes can take time to propagate.

The worker runs the task classes listed in `runner.py`; a separate Tilebox
release is not required. VM scale set image and configuration updates are manual:
let active tasks finish before reimaging instances. CPU-based autoscaling does not wake a cluster from zero or
drain Tilebox tasks automatically. Keep this example at one worker initially.

## On-prem compute and cleanup

For storage only, set `pulumi config set deployWorkers false` **before the first
deployment**. This also skips the registry. Worker-related config and a Key Vault
are unnecessary. Grant your on-prem identity Storage Blob Data Contributor on the exported container resource
ID. Use the same image and environment variables described in the main README.
Changing this setting on an existing stack removes workers and the registry;
let active tasks finish and preserve any needed images first.

Results storage is protected against accidental Pulumi deletion. Stopping or
deleting workers does not remove the dataset or its blobs. Review a destroy preview
before teardown, and retain the result storage until its catalog entries and data
are no longer needed. Do not remove protection just to make a blanket destroy pass.
