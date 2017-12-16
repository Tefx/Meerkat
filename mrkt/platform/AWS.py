from gevent.monkey import patch_all

patch_all()
import boto3
import gevent
import logging
from .base import BasePlatform


class EC2(BasePlatform):
    def __init__(self, service_cls, service_num,
                 sgroup, keyname, keyfile,
                 vm_type="t2.micro",
                 ami="ami-25632346", username="core",
                 pgroup=None, region="ap-southeast-1",
                 clean_action="stop"):
        super().__init__(service_cls, service_num)
        self.sgroup = sgroup
        self.username = username
        self.keyname = keyname
        self.keyfile = keyfile
        self.vm_type = vm_type
        self.ami = ami
        if not vm_type.startswith("t2.") or pgroup:
            self.placement = {"GroupName": pgroup}
        else:
            self.placement = {}
        self.clean_action = clean_action
        self.instances = []
        self.ec2 = boto3.resource("ec2", region_name=region)

    def finding_existing_instances(self, num):
        filters = [
            {"Name": "instance-state-name",
             'Values': ["running", "stopped"]},
            {"Name": "image-id",
             'Values': [self.ami]},
            {"Name": "instance-type",
             "Values": [self.vm_type]},
            {"Name": "tag:mrkt",
             "Values": ["True"]}
        ]
        found_instances = []
        for ins in self.ec2.instances.filter(Filters=filters):
            if ins not in self.instances:
                found_instances.append(ins)
                if len(found_instances) == num:
                    break
        return found_instances

    def launch_instances(self, num):
        tags = [{"ResourceType": "instance",
                 "Tags": [{"Key": "mrkt", "Value": "True"}]}]
        self.instances.extend(self.ec2.create_instances(
            ImageId=self.ami,
            InstanceType=self.vm_type,
            MinCount=num,
            MaxCount=num,
            KeyName=self.keyname,
            Placement=self.placement,
            SecurityGroupIds=[self.sgroup],
            TagSpecifications=tags))

    def prepare(self):
        need_instance_num = self.service_num - len(self.instances)
        exists = self.finding_existing_instances(need_instance_num)
        for ins in exists:
            if ins.state["Name"] == "stopped":
                ins.start()
        need_instance_num -= len(exists)
        logging.info("[AWS]: Preparing... %s exist, %s on platfrom, and %s new.", len(
            self.instances), len(exists), need_instance_num)
        self.instances.extend(exists)
        if need_instance_num > 0:
            self.launch_instances(need_instance_num)
        for ins in self.instances:
            ins.load()
            while (ins.state["Name"] != "running"):
                gevent.sleep(1)
                ins.load()

    def services(self):
        self.prepare()
        return [self.service_cls(ins.public_dns_name,
                                 ssh_options=dict(username=self.username,
                                                  key_filename=self.keyfile),
                                 retry_ssh=10, retry_ssh_interval=1)
                for ins in self.instances]

    def clean(self):
        if self.clean_action == "none":
            pass
        elif self.clean_action == "stop":
            for ins in self.instances:
                ins.stop()
        elif self.clean_action == "terminate":
            for ins in self.instances:
                ins.terminate()
