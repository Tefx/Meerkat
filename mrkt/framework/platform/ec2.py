from threading import current_thread
from gevent.monkey import patch_all

patch_all(thread=current_thread().name == "MainThread")
import boto3
import gevent
import urllib.request
from copy import copy
from logging import getLogger
from .local import BasePlatform
from ..service import SSHService
from ...common.utils import call_on_each
from ...common.consts import *

logger = getLogger(__name__)


def fetch_coreos_ami(region):
    url = PLATFORM_EC2_URL_AMI_COREOS.format(region=region)
    return urllib.request.urlopen(url).read().decode().strip()


class EC2(BasePlatform):
    class CleanAction:
        Null = "none"
        Stop = "stop"
        Terminate = "terminate"

    def __init__(self, srvc_dict, sgroup, keyname, keyfile,
                 ami=None, username=PLATFORM_EC2_USERNAME_DEFAULT,
                 pgroup=None, region=PLATFORM_EC2_REGION_DEFAULT,
                 clean_action=CleanAction.Stop, **options):
        super().__init__(**options)
        self.instances = []
        self.srvc_dict = srvc_dict
        self.username = username
        self.keyfile = keyfile
        self.sgroup = sgroup
        self.keyname = keyname
        self.ami = ami or fetch_coreos_ami(region)
        if pgroup:
            self.placement = {"GroupName": pgroup}
        else:
            self.placement = {}
        self.clean_action = clean_action
        self.ec2 = boto3.resource("ec2", region_name=region)
        self.pending_lets = []

    def existing_instances_on_platform(self):
        filters = [
            {"Name": "instance-state-name",
             'Values': ["running", "stopped"]},
            {"Name": "image-id",
             'Values': [self.ami]},
            {"Name": "instance-type",
             "Values": list(self.srvc_dict.keys())},
            {"Name": "tag:{}".format(PLATFORM_EC2_INSTANCE_TAG),
             "Values": ["True"]}
        ]
        return [ins for ins in self.ec2.instances.filter(Filters=filters)
                if ins not in self.instances]

    def prepare_instances(self):
        srvc_dict = copy(self.srvc_dict)
        logger.info("[AWS]Preparing VMs: Needs %s", srvc_dict)
        for ins in self.instances:
            if srvc_dict.get(ins.instance_type) > 0:
                srvc_dict[ins.instance_type] -= 1
            else:
                getattr(ins, self.clean_action)()
        logger.info("[AWS]Preparing VMs: Not connected %s", srvc_dict)
        for ins in self.existing_instances_on_platform():
            if srvc_dict.get(ins.instance_type) > 0:
                srvc_dict[ins.instance_type] -= 1
                self.instances.append(ins)
                if ins.state["Name"] == "stopped":
                    ins.start()
        logger.info("[AWS]Preparing VMs: New launch %s", srvc_dict)
        tags = [{"ResourceType": "instance",
                 "Tags": [{"Key": PLATFORM_EC2_INSTANCE_TAG, "Value": "True"}]}]
        for vm_type, num in srvc_dict.items():
            if num > 0:
                self.instances.extend(
                    self.ec2.create_instances(
                        ImageId=self.ami,
                        InstanceType=vm_type,
                        MinCount=num,
                        MaxCount=num,
                        KeyName=self.keyname,
                        Placement=self.placement,
                        SecurityGroupIds=[self.sgroup],
                        TagSpecifications=tags))

    def create_service(self, instance, options):
        instance.load()
        while instance.state["Name"] != "running":
            gevent.sleep(PLATFORM_EC2_INSTANCE_READY_WAIT_INTERVAL)
            instance.load()
        service = SSHService(instance.public_dns_name, username=self.username, key_filename=self.keyfile)
        service.set_options(
            dict(retry_ssh=PLATFORM_EC2_SSH_RETRY_TIMES, retry_ssh_interval=PLATFROM_EC2_SSH_RETRY_INTERVAL), options,
            self.options)
        service.prepare_workers()
        self.services.append(service)

    def prepare_services(self, options):
        self.prepare_instances()
        for ins in self.instances:
            self.pending_lets.append(gevent.spawn(self.create_service, ins, options))

    def clean(self):
        gevent.joinall(self.pending_lets)
        call_on_each(self.services, "clean", join=True)
        if self.clean_action != self.CleanAction.Null:
            call_on_each(self.instances, self.clean_action, join=True)
