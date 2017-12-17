from gevent.monkey import patch_all

patch_all()
import os
import os.path
import paramiko
import math
import logging
import json
from gevent import sleep

from . import agent
from .utils import set_option

AGENT_RUN_CMD = "mrkt-agent -p {in_port} -l info ."
DOCKER_RUN_CMD = "docker run -itd --name {name} -p {out_port}:{in_port} {image} {engine_start_cmd}"
DOCKER_RM_CMD = "docker rm -f {name}"
DOCKER_INSTALL_IMAGE_CMD = "gunzip -c {image} | docker load && rm {image}"
DOCKER_UNINSTALLL_IMAGE_CMD = "docker rmi {image}"
DOCKER_CONTAINERS = "docker container ls --format \"{{json .}}\""
DOCKER_IMAGES = "docker images --format \"{{json .Repository}}\""


class BaseService:
    def __init__(self, addr, **options):
        self.addr = addr
        self.workers = []
        self.update_options(options)

    def update_options(self, options):
        set_option(self, "worker_limit", None, options)
        set_option(self, "image", None, options)
        set_option(self, "image_archive", None, options)
        set_option(self, "image_update", True, options)
        set_option(self, "image_clean", True, options)

    def prepare(self):
        self.connect()
        self.install_image()

    def connect(self):
        pass

    @property
    def free_slot_number(self):
        return self.worker_limit - len(self.workers)

    def install_image(self):
        raise NotImplementedError

    def uninstall_image(self):
        raise NotImplementedError

    def start_workers(self, num=math.inf):
        raise NotImplementedError

    def stop_workers(self):
        raise NotImplementedError

    def clean(self):
        if self.workers:
            self.stop_workers()
        if self.image and self.image_clean:
            self.uninstall_image()


class DockerViaSSH(BaseService):
    def __init__(self, addr, **options):
        super(DockerViaSSH, self).__init__(addr, **options)
        self.ssh_client = None
        self.dockers = []

    def update_options(self, options):
        super(DockerViaSSH, self).update_options(options)
        set_option(self, "ssh_options", {}, options)
        set_option(self, "retry_ssh", 1, options)
        set_option(self, "retry_ssh_interval", 0, options)

    def try_ssh_connect(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        times = 0
        last_exception = None
        while times < self.retry_ssh:
            logging.info("[SSH]: [%s/%s] %s with %s", times + 1, self.retry_ssh, self.addr, self.ssh_options)
            try:
                ssh_client.connect(self.addr, **self.ssh_options)
                logging.info("[SSH]: %s connected", self.addr)
                return ssh_client
            except paramiko.ssh_exception.NoValidConnectionsError as e:
                last_exception = e
            times += 1
            sleep(self.retry_ssh_interval)
        raise last_exception

    def connect(self):
        self.ssh_client = self.try_ssh_connect()
        if not self.worker_limit:
            self.worker_limit = int(self.ssh_exec("nproc"))

    def ssh_exec(self, cmd):
        logging.info("[EXEC]%s: %s", self.addr, cmd)
        _, out, err = self.ssh_client.exec_command(cmd)
        if out.channel.recv_exit_status() == 0:
            out = out.read().decode()
            logging.debug("[ERET]%s: %s", self.addr, out)
            return out
        else:
            logging.critical("[EXEC]%s: %s", self.addr, cmd)
            logging.critical("[EXEC]%s: %s", self.addr, err.read().decode())
            return None

    def install_image(self):
        if not self.image_update:
            image = self.image or os.path.basename(self.image_archive).split(".")[0]
            if self.image_exists(image or self.image):
                self.image = image
                return
        if self.image and self.image_exists(self.image):
            self.kill_dockers(self.existing_dockers(image=self.image))
            self.uninstall_image(self.image)
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

    def uninstall_image(self, image=None):
        self.ssh_exec(DOCKER_UNINSTALLL_IMAGE_CMD.format(
            image=image or self.image))
        self.image = None

    def image_exists(self, name):
        for line in self.ssh_exec(DOCKER_IMAGES).splitlines():
            image = json.loads(line)
            if image.startswith(name):
                return True
        return False

    def existing_dockers(self, image):
        dockers = []
        for line in self.ssh_exec(DOCKER_CONTAINERS).splitlines():
            container = json.loads(line)
            if container["Image"].startswith(image):
                dockers.append(container["Names"])
        return dockers

    def kill_dockers(self, dockers=None):
        dockers = dockers or self.dockers
        if dockers:
            self.ssh_exec(DOCKER_RM_CMD.format(name=" ".join(dockers)))

    def start_docker(self, out_port):
        self.kill_dockers(self.existing_dockers(image=self.image))
        port = agent.DEFAULT_PORT
        name = "mrkt_{}".format(out_port)
        engine_start_cmd = AGENT_RUN_CMD.format(in_port=port)
        docker_start_cmd = DOCKER_RUN_CMD.format(
            name=name, image=self.image, engine_start_cmd=engine_start_cmd,
            in_port=port, out_port=out_port)
        if self.ssh_exec(docker_start_cmd) != None:
            return name

    def start_workers(self, num=math.inf):
        num = min(self.free_slot_number, num)
        self.dockers = [self.start_docker(agent.DEFAULT_PORT)]
        self.workers = [agent.Client((self.addr, agent.DEFAULT_PORT)) for _ in range(num)]
        return self.workers

    def stop_workers(self):
        self.kill_dockers()
        self.workers = []


class MultiDockerViaSSH(DockerViaSSH):
    def start_workers(self, num=math.inf):
        num = min(self.free_slot_number, num)
        while len(self.workers) < num:
            out_port = agent.DEFAULT_PORT + len(self.dockers)
            self.dockers.append(self.start_docker(out_port))
            self.workers.append(agent.Client((self.addr, out_port)))
        return self.workers
