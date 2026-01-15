import pulumi
import pulumi_opentelekomcloud as otc
from pulumi import ResourceOptions

tailscale_config = pulumi.Config("tailscale")

workflow_bucket = otc.ObsBucket(
    "workflow-storage", bucket="s2-clay", acl="private", versioning=False, storage_class="WARM"
)

subnet = otc.get_vpc_subnet_v1(name="subnet-default")  # pre-created by OTC


user_data_script = pulumi.Output.format(
    """#!/bin/bash
# Enable command logging for debugging (check /var/log/cloud-init-output.log)
set -x
apt-get update -y
apt-get install -y curl git fish

curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable --now tailscaled
tailscale up --authkey {tailscale_auth_key} --ssh --hostname=s2-clay-cpu-worker-01
""",
    tailscale_auth_key=tailscale_config.require_secret("authKey"),
)

ubuntu_image = otc.get_images_image_v2(
    name_regex="^Standard_Ubuntu_24.04.*_uefi_latest$", most_recent=True, visibility="public"
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

instance = otc.ComputeInstanceV2(
    "s2-clay-cpu-worker-01",
    name="s2-clay-cpu-worker-01",
    flavor_id="c7n.2xlarge.4",
    availability_zone="eu-nl-01",
    networks=[{"uuid": subnet.id}],
    user_data=user_data_script,
    block_devices=[
        {
            "source_type": "image",  # the source image to boot from
            "uuid": ubuntu_image.id,  # id of the boot image
            "destination_type": "volume",  # to make it a persistent volume
            "boot_index": 0,  # this is the Boot Disk
            "volume_size": 128,  # 128 GB
            "delete_on_termination": True,  # Delete disk when server is destroyed
            "volume_type": "SSD",  # Optional: SSD (Ultra-High I/O) or SATA (Common I/O)
        }
    ],
)
