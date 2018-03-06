import mrkt.agent.worker

from threading import current_thread
from logging import getLogger
logger = getLogger(__name__)
from gevent.monkey import patch_all

patch_all(thread=current_thread().name == "MainThread")

from . import agent

import os
import os.path
import paramiko
import json
from gevent import sleep

AGENT_RUN_CMD = "mrkt-agent -p {in_port} -l debug ."
DOCKER_RUN_CMD = "docker run -itd --name {name} -p {out_port}:{in_port} {image} {engine_start_cmd}"
DOCKER_RM_CMD = "docker rm -f {name}"
DOCKER_INSTALL_IMAGE_CMD = "gunzip -c {image} | docker load && rm {image}"
DOCKER_UPDATE_IMAGE_CMD = "docker pull {image}"
DOCKER_UNINSTALLL_IMAGE_CMD = "docker rmi {image}"
DOCKER_CONTAINERS = "docker container ls -a --format \"{{json .Names}}\""
DOCKER_IMAGES = "docker images --format \"{{json .Repository}}\""
DOCKER_LIST_NONE_IMAGES_CMD = "docker images | grep '<none>' | awk '{print $3}'"


class BaseService:
    def __init__(self, addr):
        self.addr = addr
        self.workers = []
        self.image = None
        self.image_archive = None
        self.image_update = True
        self.image_clean = False

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

    def connect(self):
        pass

    def install_image(self):
        raise NotImplementedError

    def uninstall_image(self, image=None):
        raise NotImplementedError

    def start_workers(self, num=1):
        raise NotImplementedError

    def stop_workers(self):
        raise NotImplementedError

    def clean(self):
        if self.workers:
            self.stop_workers()
        if self.image and self.image_clean:
            self.uninstall_image()


class SSHService(BaseService):
    def __init__(self, addr, **ssh_options):
        super(SSHService, self).__init__(addr)
        self.ssh_client = None
        self.dockers = []
        self.ssh_options = ssh_options
        self.retry_ssh = 2
        self.retry_ssh_interval = 1
        self.worker_port = agent.DEFAULT_PORT

    def set_options(self, *options_list):
        ssh_options = self.ssh_options
        super(SSHService, self).set_options(*options_list)
        self.ssh_options.update(ssh_options)

    def try_ssh_connect(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        times = 0
        last_exception = Exception()
        while times < self.retry_ssh:
            logger.info("[SSH]: [%s/%s] %s with %s", times + 1,
                         self.retry_ssh, self.addr, self.ssh_options)
            try:
                ssh_client.connect(self.addr, **self.ssh_options)
                logger.info("[SSH]: %s connected", self.addr)
                return ssh_client
            except paramiko.ssh_exception.NoValidConnectionsError as xcp:
                last_exception = xcp
            times += 1
            sleep(self.retry_ssh_interval)
        raise last_exception

    def connect(self):
        self.ssh_client = self.try_ssh_connect()

    def close(self):
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None

    def ssh_exec(self, cmd):
        logger.info("[EXEC]%s: %s", self.addr, cmd)
        _, out, err = self.ssh_client.exec_command(cmd)
        if out.channel.recv_exit_status() != 0:
            logger.critical("[EXEC]%s: %s", self.addr, cmd)
            logger.critical("[EXEC]%s: %s", self.addr, err.read().decode())
            return None
        out = out.read().decode()
        logger.debug("[ERET]%s: %s", self.addr, out)
        return out

    def install_image(self):
        if not self.image_update:
            image = self.image or os.path.basename(
                self.image_archive).split(".")[0]
            if self.image_exists(image):
                self.image = image
                return
        if self.image_archive:
            sftp = paramiko.SFTPClient.from_transport(
                self.ssh_client.get_transport())
            file_name = os.path.basename(self.image_archive)
            sftp.put(self.image_archive, os.path.join("", file_name))
            out = self.ssh_exec(
                DOCKER_INSTALL_IMAGE_CMD.format(image=file_name))
            for line in out.splitlines():
                if line.startswith("Loaded image:"):
                    self.image = line[13:].strip()
        else:
            self.ssh_exec(DOCKER_UPDATE_IMAGE_CMD.format(image=self.image))
        self.clean_legacy_images()

    def clean_legacy_images(self):
        images = []
        for line in self.ssh_exec(DOCKER_LIST_NONE_IMAGES_CMD).splitlines():
            images.append(line.strip())
        if images:
            self.ssh_exec(DOCKER_UNINSTALLL_IMAGE_CMD.format(
                image=" ".join(images)))

    def uninstall_image(self, image=None):
        self.ssh_exec(DOCKER_UNINSTALLL_IMAGE_CMD.format(
            image=image or self.image))
        self.image = None

    def image_exists(self, name):
        for line in self.ssh_exec(DOCKER_IMAGES).splitlines():
            image = json.loads(line)
            if ":" not in name:
                name += ":latest"
            if ":" not in image:
                image += ":latest"
            if name == image:
                return True
        return False

    def existing_dockers(self):
        dockers = []
        for line in self.ssh_exec(DOCKER_CONTAINERS).splitlines():
            container_name = json.loads(line)
            if container_name.startswith("mrkt"):
                dockers.append(container_name)
        return dockers

    def kill_dockers(self, dockers=None):
        dockers = dockers or self.dockers
        if dockers:
            self.ssh_exec(DOCKER_RM_CMD.format(name=" ".join(dockers)))

    def start_docker(self, out_port):
        self.kill_dockers(self.existing_dockers())
        port = agent.DEFAULT_PORT
        name = "mrkt_{}".format(out_port)
        engine_start_cmd = AGENT_RUN_CMD.format(in_port=port)
        docker_start_cmd = DOCKER_RUN_CMD.format(
            name=name, image=self.image, engine_start_cmd=engine_start_cmd,
            in_port=port, out_port=out_port)
        return name if self.ssh_exec(docker_start_cmd) else None

    def start_workers(self, num=1):
        self.dockers = [self.start_docker(agent.DEFAULT_PORT)]
        self.workers = [mrkt.agent.worker.Worker(
            (self.addr, self.worker_port)) for _ in range(num)]
        return self.workers

    def stop_workers(self):
        self.kill_dockers()
        self.workers = []

    def copy_workers(self):
        return [mrkt.agent.worker.Worker((self.addr, self.worker_port))
                for _ in self.workers]
