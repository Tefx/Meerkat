from threading import current_thread
from gevent.monkey import patch_all

patch_all(thread=current_thread().name == "MainThread")
import boto3
import gevent
import logging
from .base import BasePlatform
import urllib.request
from copy import copy

COREOS_AMI_URL = "https://stable.release.core-os.net/amd64-usr/current/coreos_production_ami_hvm_{region}.txt"


def fetch_coreos_ami(region):
    url = COREOS_AMI_URL.format(region=region)
    return urllib.request.urlopen(url).read().decode().strip()


class EC2(BasePlatform):
    def __init__(self, service_cls, service_dict,
                 sgroup, keyname, keyfile,
                 ami=None, username="core",
                 pgroup=None, region="ap-southeast-1",
                 clean_action="stop"):
        super().__init__(service_cls)
        self.service_dict = service_dict
        self.sgroup = sgroup
        self.username = username
        self.keyname = keyname
        self.keyfile = keyfile
        self.ami = ami or fetch_coreos_ami(region)
        if pgroup:
            self.placement = {"GroupName": pgroup}
        else:
            self.placement = {}
        self.clean_action = clean_action
        self.instances = []
        self.ec2 = boto3.resource("ec2", region_name=region)

    def existing_instances_on_platform(self):
        filters = [
            {"Name": "instance-state-name",
             'Values': ["running", "stopped"]},
            {"Name": "image-id",
             'Values': [self.ami]},
            {"Name": "instance-type",
             "Values": list(self.service_dict.keys())},
            {"Name": "tag:mrkt",
             "Values": ["True"]}
        ]
        return [ins for ins in self.ec2.instances.filter(Filters=filters)
                if ins not in self.instances]

    def launch_instances(self, service_dict):
        tags = [{"ResourceType": "instance",
                 "Tags": [{"Key": "mrkt", "Value": "True"}]}]
        for vm_type, num in service_dict.items():
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

    def prepare_instances(self):
        service_dict = copy(self.service_dict)
        logging.info("[AWS]Preparing VMs: Needs %s", service_dict)
        for ins in self.instances:
            if service_dict.get(ins.instance_type) > 0:
                service_dict[ins.instance_type] -= 1
            else:
                self.clean(instances=[ins])
        logging.info("[AWS]Preparing VMs: Not connected %s", service_dict)
        for ins in self.existing_instances_on_platform():
            if service_dict.get(ins.instance_type) > 0:
                service_dict[ins.instance_type] -= 1
                self.instances.append(ins)
                if ins.state["Name"] == "stopped":
                    ins.start()
        logging.info("[AWS]Preparing VMs: New launch %s", service_dict)
        self.launch_instances(service_dict)
        for ins in self.instances:
            ins.load()
            while ins.state["Name"] != "running":
                gevent.sleep(1)
                ins.load()

    def services(self):
        self.prepare_instances()
        return [self.service_cls(ins.public_dns_name,
                                 ssh_options=dict(username=self.username,
                                                  key_filename=self.keyfile),
                                 retry_ssh=10, retry_ssh_interval=1)
                for ins in self.instances]

    def clean(self, instances=None):
        if instances == None:
            instances = self.instances
        if self.clean_action == "none":
            pass
        elif self.clean_action == "stop":
            for ins in instances:
                ins.stop()
        elif self.clean_action == "terminate":
            for ins in instances:
                ins.terminate()
