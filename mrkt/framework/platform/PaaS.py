from ...common.utils import patch;

patch()
import gevent
from gevent.pool import Group
from copy import copy
from logging import getLogger
from ..role import Platform
from ...common.utils import call_on_each
from ...common.consts import *

logger = getLogger(__name__)


class PaaS(Platform):
    class CleanAction:
        Null = "none"
        Stop = "stop"
        Terminate = "terminate"

    def __init__(self, requests, clean_action=CleanAction.Stop, **options):
        super().__init__(**options)
        self.VMs = []
        self.requests = requests
        self.clean_action = clean_action
        self.pending_lets = Group()

    def VMs_on_platform(self):
        raise NotImplementedError

    def launch_VMs(self, vm_type, vm_num):
        raise NotImplementedError

    def VM_is_ready(self, vm):
        raise NotImplementedError

    def service_on_VM(self, vm):
        raise NotImplementedError

    def clean_VM(self, vm):
        if self.clean_action != self.CleanAction.Null:
            raise NotImplementedError

    def prepare_VMs(self):
        provisioning_requests = copy(self.requests)
        logger.info("[%s.prepare_VMs]: Requesting %s",
                    self.__class__.__name__,
                    provisioning_requests)
        for vm in self.VMs:
            if provisioning_requests.get(vm.instance_type) > 0:
                provisioning_requests[vm.instance_type] -= 1
            else:
                self.clean_VM(vm)
        logger.info("[%s.prepare_VMs]: Not connected %s",
                    self.__class__.__name__,
                    provisioning_requests)
        for vm in self.VMs_on_platform():
            if vm not in self.VMs and provisioning_requests.get(vm.instance_type) > 0:
                provisioning_requests[vm.instance_type] -= 1
                self.VMs.append(vm)
                if vm.state["Name"] == "stopped":
                    vm.start()
        logger.info("[%s.prepare_VMs]: New launch %s",
                    self.__class__.__name__,
                    provisioning_requests)
        for vm_type, vm_num in provisioning_requests.items():
            if vm_num:
                self.VMs.extend(self.launch_VMs(vm_type, vm_num))
        return self.VMs

    def create_service(self, vm, options):
        while not self.VM_is_ready(vm):
            gevent.sleep(PLATFORM_PAAS_VM_WAIT_INTERVAL)
        service = self.service_on_VM(vm)
        service.set_options(
            dict(retry_ssh=PLATFORM_PAAS_SSH_RETRIES,
                 retry_ssh_interval=PLATFORM_PAAS_SSH_RETRY_INTERVAL),
            options,
            self.options)
        service.prepare_workers()
        self.services.append(service)

    def prepare_services(self, options):
        for vm in self.prepare_VMs():
            self.pending_lets.spawn(self.create_service, vm, options)

    def clean(self):
        self.pending_lets.join()
        call_on_each(self.services, "clean", join=True)
        self.pending_lets.map(self.clean_VM, self.VMs)
        self.pending_lets.join()
