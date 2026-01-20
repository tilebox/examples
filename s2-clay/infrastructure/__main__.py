import pulumi
import pulumi_opentelekomcloud as otc
from pulumi import ResourceOptions

runner_config = pulumi.Config("runner")
tailscale_config = pulumi.Config("tailscale")

workflow_bucket = otc.ObsBucket(
    "workflow-storage", bucket="s2-clay", acl="private", versioning=False, storage_class="WARM"
)

subnet = otc.get_vpc_subnet_v1(name="subnet-default")  # pre-created by OTC

user_data_script = """#!/bin/bash
# Enable command logging for debugging (check /var/log/cloud-init-output.log)
set -x
apt-get update -y
apt-get install -y curl git git-lfs fish tmux {extra_packages}
git lfs install

cat <<EOF > /etc/profile.d/tilebox-worker.sh
RUNNER_NAME={hostname}
AXIOM_API_KEY={axiom_api_key}
TILEBOX_API_KEY={tilebox_api_key}
AXIOM_TRACES_DATASET=workflow-traces
AXIOM_LOGS_DATASET=workflow-logs
OTC_ACCESS_KEY_ID={otc_access_key_id}
OTC_SECRET_ACCESS_KEY={otc_secret_access_key}
COPERNICUS_ACCESS_KEY_ID={copernicus_access_key_id}
COPERNICUS_SECRET_ACCESS_KEY={copernicus_secret_access_key}
EOF

# Make sure it is executable
chmod 644 /etc/profile.d/tilebox-worker.sh

# Load them immediately for the rest of this script (cloud-init runs as root)
source /etc/profile.d/tilebox-worker.sh

curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
tailscale up --authkey {tailscale_auth_key} --ssh --hostname={hostname}

su - ubuntu <<'EOF'
    echo "I am running as: $(whoami)"
    # install uv
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # install go (via mise)
    curl https://mise.run | sh
    echo 'eval "$(~/.local/bin/mise activate bash)"' >> ~/.bashrc
    eval "$(~/.local/bin/mise activate bash)"
    mise use -g go@1.25.5

    # install call-in-parallel
    go install github.com/tilebox/call-in-parallel@latest
EOF
"""


ubuntu_image = otc.get_images_image_v2(
    name_regex="^Standard_Ubuntu_24.04.*_uefi_latest$", most_recent=True, visibility="public"
)

ubuntu_gpu_image = otc.get_images_image_v2(
    name_regex="^Standard_Ubuntu_24.04.*_GPU_latest$", most_recent=True, visibility="public"
)


# Outbound internet connectivity
# Create a public IP
nat_elastic_ip = otc.VpcEipV1(
    "nat-gateway-ip",
    publicip={"type": "5_bgp"},
    bandwidth={
        "name": "nat-gateway-bandwidth",
        "size": 500,  # MB/s
        "share_type": "PER",
        "charge_mode": "traffic",
    },
)

# a NAT gateway which uses the public IP
nat_gateway = otc.NatGatewayV2(
    "nat-gateway",
    name="nat-gateway",
    router_id=subnet.vpc_id,
    internal_network_id=subnet.id,
    spec="1",  # small # 0: micro, 1: small, 2: medium, 3: large, 4: extra-large
    region="eu-nl",
    opts=ResourceOptions(depends_on=[nat_elastic_ip]),
)

# a source nat rule which allows all traffic from the subnet to use the NAT gateway
snat_rule = otc.NatSnatRuleV2(
    "s2-clay-snat-rule",
    nat_gateway_id=nat_gateway.id,
    network_id=subnet.id,  # Applies to all VMs in this subnet
    floating_ip_id=nat_elastic_ip.id,
    opts=ResourceOptions(depends_on=[nat_elastic_ip, nat_gateway]),
)


class ComputeInstance(pulumi.ComponentResource):
    def __init__(
        self,
        name: str,
        flavor_id: str,
        image_id: str,
        extra_packages: list[str] | None = None,
        opts: ResourceOptions | None = None,
    ) -> None:
        super().__init__("tilebox:opentelekomcloud:ComputeInstance", name, opts=opts)

        user_data = pulumi.Output.format(
            user_data_script,
            extra_packages=" ".join(extra_packages) if extra_packages else "",
            hostname=f"s2-clay-{name}",
            tailscale_auth_key=tailscale_config.require_secret("authKey"),
            tilebox_api_key=runner_config.require_secret("tileboxApiKey"),
            axiom_api_key=runner_config.require_secret("axiomApiKey"),
            otc_access_key_id=runner_config.require_secret("otcAccessKeyId"),
            otc_secret_access_key=runner_config.require_secret("otcSecretAccessKey"),
            copernicus_access_key_id=runner_config.require_secret("copernicusAccessKeyId"),
            copernicus_secret_access_key=runner_config.require_secret("copernicusSecresAccessKey"),
        )

        self.instance = otc.ComputeInstanceV2(
            name,
            name=name,
            flavor_id=flavor_id,
            availability_zone="eu-nl-01",
            networks=[{"uuid": subnet.id}],
            user_data=user_data,
            block_devices=[
                {
                    "source_type": "image",  # the source image to boot from
                    "uuid": image_id,  # id of the boot image
                    "destination_type": "volume",  # to make it a persistent volume
                    "boot_index": 0,  # this is the Boot Disk
                    "volume_size": 100,  # 128 GB
                    "delete_on_termination": True,  # Delete disk when server is destroyed
                    "volume_type": "SSD",  # Optional: SSD (Ultra-High I/O) or SATA (Common I/O)
                }
            ],
            opts=ResourceOptions(parent=self, depends_on=[snat_rule]),
        )
        self.register_outputs({"instance_id": self.instance.id})


n_cpu_workers = 0
for i in range(1, n_cpu_workers + 1):
    ComputeInstance(f"cpu-worker-{i:02d}", "c7n.2xlarge.4", ubuntu_image.id)


n_gpu_workers = 1
for i in range(1, n_gpu_workers + 1):
    ComputeInstance(
        f"gpu-worker-{i:02d}",
        "pi2.2xlarge.4",
        ubuntu_gpu_image.id,
        # nvidia-driver-570-server is the LTS version of the NVIDIA drivers for Ubuntu 24.04 for CUDA 12.X
        extra_packages=["nvidia-driver-570-server", "nvidia-utils-570-server"],
    )
