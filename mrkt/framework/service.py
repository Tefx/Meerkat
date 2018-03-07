from threading import current_thread
from gevent.monkey import patch_all

patch_all(thread=current_thread().name == "MainThread")
import os
import os.path
import paramiko
import json
from gevent import sleep
from logging import getLogger
from .worker import Worker
from ..common.consts import *

logger = getLogger(__name__)

CMD_AGENT_START = "mrkt-agent -p {in_port} -l debug ."
CMD_DOCKER_START_CONTAINER = "docker run -itd --name {name} -p {out_port}:{in_port} {image} {engine_start_cmd}"
CMD_DOCKER_RM_CONTAINER = "docker rm -f {name}"
CMD_DOCKER_INSTALL_IMAGE = "gunzip -c {image} | docker load && rm {image}"
CMD_DOCKER_UPDATE_IMAGE = "docker pull {image}"
CMD_DOCKER_UNINSTALLL_IMAGE = "docker rmi {image}"
CMD_DOCKER_LS_CONTAINERS = "docker container ls -a --format \"{{json .Names}}\""
CMD_DOCKER_LS_IMAGES = "docker images --format \"{{json .Repository}}\""
CMD_DOCKER_LIST_NONE_IMAGES = "docker images | grep '<none>' | awk '{print $3}'"


class BaseService:
    def __init__(self, addr):
        self.addr = addr
        self.workers = []
        self.image = SERVICE_CONTAINER_IMAGE_DEFAULT
        self.image_archive = SERVICE_CONTAINER_IMAGE_ARCHIVE_DEFAULT
        self.image_update = SERVICE_CONTAINER_IMAGE_UPDATE_DEFAULT
        self.image_clean = SERVICE_CONTAINER_IMAGE_CLEAN_DEFAULT

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
        self.retry_ssh = SERVICE_SSH_RETRY_TIMES
        self.retry_ssh_interval = SERVICE_SSH_RETRY_INTERVAL
        self.worker_port = AGENT_PORT

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
            if self.image_archive:
                image = os.path.basename(self.image_archive).split(".")[0]
            else:
                image = self.image
            if self.image_exists(image):
                self.image = image
                return
        if self.image_archive:
            sftp = paramiko.SFTPClient.from_transport(
                self.ssh_client.get_transport())
            file_name = os.path.basename(self.image_archive)
            sftp.put(self.image_archive, os.path.join("", file_name))
            out = self.ssh_exec(
                CMD_DOCKER_INSTALL_IMAGE.format(image=file_name))
            for line in out.splitlines():
                if line.startswith("Loaded image:"):
                    self.image = line[13:].strip()
        else:
            self.ssh_exec(CMD_DOCKER_UPDATE_IMAGE.format(image=self.image))
        self.clean_legacy_images()

    def clean_legacy_images(self):
        images = []
        for line in self.ssh_exec(CMD_DOCKER_LIST_NONE_IMAGES).splitlines():
            images.append(line.strip())
        if images:
            self.ssh_exec(CMD_DOCKER_UNINSTALLL_IMAGE.format(
                image=" ".join(images)))

    def uninstall_image(self, image=None):
        self.ssh_exec(CMD_DOCKER_UNINSTALLL_IMAGE.format(
            image=image or self.image))
        self.image = None

    def image_exists(self, name):
        for line in self.ssh_exec(CMD_DOCKER_LS_IMAGES).splitlines():
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
        for line in self.ssh_exec(CMD_DOCKER_LS_CONTAINERS).splitlines():
            container_name = json.loads(line)
            if container_name.startswith(SERVICE_CONTAINER_NAME_PREFIX):
                dockers.append(container_name)
        return dockers

    def kill_dockers(self, dockers=None):
        dockers = dockers or self.dockers
        if dockers:
            self.ssh_exec(CMD_DOCKER_RM_CONTAINER.format(name=" ".join(dockers)))

    def start_docker(self, out_port):
        self.kill_dockers(self.existing_dockers())
        port = AGENT_PORT
        name = "{}_{}".format(SERVICE_CONTAINER_NAME_PREFIX, out_port)
        engine_start_cmd = CMD_AGENT_START.format(in_port=port)
        docker_start_cmd = CMD_DOCKER_START_CONTAINER.format(
            name=name, image=self.image, engine_start_cmd=engine_start_cmd,
            in_port=port, out_port=out_port)
        return name if self.ssh_exec(docker_start_cmd) else None

    def start_workers(self, num=1):
        self.dockers = [self.start_docker(AGENT_PORT)]
        self.workers = [Worker((self.addr, self.worker_port)) for _ in range(num)]
        return self.workers

    def stop_workers(self):
        self.kill_dockers()
        self.workers = []

    def copy_workers(self):
        return [Worker((self.addr, self.worker_port)) for _ in self.workers]
