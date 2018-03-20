from ...common.utils import patch;

patch()
import boto3
import urllib.request

from .PaaS import PaaS
from ..service import docker
from ...common.consts import *

COREOS_AMI_URL = "https://stable.release.core-os.net/amd64-usr/current/coreos_production_ami_hvm_{region}.txt"
COREOS_USERNAME = "core"
VM_TAG = [{"ResourceType": "instance",
           "Tags":         [{"Key":   PLATFORM_EC2_VM_TAG,
                             "Value": "True"}]}]


def fetch_coreos_ami(region):
    url = COREOS_AMI_URL.format(region=region)
    return urllib.request.urlopen(url).read().decode().strip()


class EC2(PaaS):
    def __init__(self, requests, sgroup, key_name, key_file,
                 ami=None,
                 username=COREOS_USERNAME,
                 placement_group=None,
                 region=PLATFORM_EC2_REGION,
                 clean_action=PaaS.CleanAction.Stop,
                 **options):
        super(EC2, self).__init__(requests, clean_action, **options)
        self.sgroup = sgroup
        self.ami = ami or fetch_coreos_ami(region)
        self.key_name = key_name
        self.key_file = key_file
        self.username = username
        self.placement = {"GroupName": placement_group} if placement_group else {}
        self.ec2_client = boto3.resource("ec2", region_name=region)

    def VMs_on_platform(self):
        filters = [
            {"Name":   "instance-state-name",
             'Values': ["running", "stopped"]},
            {"Name":   "image-id",
             'Values': [self.ami]},
            {"Name":   "instance-type",
             "Values": list(self.requests.keys())},
            {"Name":   "tag:{}".format(PLATFORM_EC2_VM_TAG),
             "Values": ["True"]}
        ]
        return self.ec2_client.instances.filter(Filters=filters)

    def launch_VMs(self, vm_type, vm_num):
        return self.ec2_client.create_instances(ImageId=self.ami,
                                                InstanceType=vm_type,
                                                MinCount=vm_num,
                                                MaxCount=vm_num,
                                                KeyName=self.key_name,
                                                Placement=self.placement,
                                                SecurityGroupIds=[self.sgroup],
                                                TagSpecifications=VM_TAG)

    def VM_is_ready(self, vm):
        vm.load()
        return vm.state["Name"] == "running"

    def service_on_VM(self, vm):
        return docker.ViaSSH(vm.public_dns_name,
                             username=self.username,
                             key_filename=self.key_file)

    def clean_VM(self, vm):
        if self.clean_action != self.CleanAction.Null:
            getattr(vm, self.clean_action)()
