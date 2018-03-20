import os
import os.path

from .worker import Worker
from ...common.consts import *


class Service:

    def connect(self):
        raise NotImplementedError

    def existing_images(self, only_outdated=False):
        raise NotImplementedError

    def install_image_via_archive(self, archive):
        raise NotImplementedError

    def install_image_via_name(self, image_name):
        raise NotImplementedError

    def uninstall_images(self, images):
        raise NotImplementedError

    def existing_containers(self):
        raise NotImplementedError

    def start_containers(self, out_port):
        raise NotImplementedError

    def kill_containers(self, dockers=None):
        raise NotImplementedError

    def __init__(self, address):
        self.address = address
        self.containers = []
        self.workers = []
        self.worker_port = AGENT_PORT
        self.image = SERVICE_DOCKER_IMAGE
        self.image_archive = SERVICE_IMAGE_ARCHIVE
        self.image_update = SERVICE_IMAGE_UPDATE
        self.image_clean = SERVICE_IMAGE_CLEAN

    def set_options(self, *options_list):
        for options in options_list:
            for option, value in options.items():
                if hasattr(self, option):
                    setattr(self, option, value)

    def prepare_workers(self):
        self.connect()
        self.install_image()
        if self.workers:
            self.stop_workers()
        self.start_workers()

    def clean(self):
        if self.workers:
            self.stop_workers()
        if self.image and self.image_clean:
            self.uninstall_images([self.image])
            self.image = None

    def start_workers(self, num=1):
        ex_containers = [c for c in self.existing_containers()
                         if c.startswith(SERVICE_CONTAINER_PREFIX)]
        self.kill_containers(ex_containers)
        self.containers = [self.start_containers(AGENT_PORT)]
        self.workers = [Worker((self.address, self.worker_port)) for _ in range(num)]
        return self.workers

    def stop_workers(self):
        for worker in self.workers:
            worker.clean()
        self.workers = []
        self.kill_containers()

    def image_exists(self, name):
        for image in self.existing_images():
            if ":" not in name:
                name += ":latest"
            if ":" not in image:
                image += ":latest"
            if name == image:
                return True
        return False

    def install_image(self):
        if self.image_archive:
            image_name = os.path.basename(self.image_archive).split(".")[0]
            if not self.image_exists(image_name) or self.image_update:
                self.image = self.install_image_via_archive(self.image_archive)
        elif self.image:
            if not self.image_exists(self.image) or self.image_update:
                self.image = self.install_image_via_name(self.image)
        else:
            raise AttributeError
        outdated_images = self.existing_images(only_outdated=True)
        self.uninstall_images(outdated_images)
