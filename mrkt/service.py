from gevent.monkey import patch_all
patch_all()
import os
import os.path
import paramiko
import math
import logging
import json

from . import engine

ENGINE_RUN_CMD = "mrkt-ng -p {in_port} {entry_points}"
DOCKER_RUN_CMD = "docker run -d -t --name {name} -p {out_port}:{in_port} {image} {engine_start_cmd}"
DOCKER_RM_CMD = "docker rm -f {name}"
DOCKER_INSTALL_IMAGE_CMD = "gunzip -c {image} | docker load && rm {image}"
DOCKER_UNINSTALLL_IMAGE_CMD = "docker rmi {image}"
DOCKER_CONTAINERS = "docker container ls --format \"{{json .}}\""
DOCKER_IMAGES = "docker images --format \"{{json .}}\""


class BaseService:
    def __init__(self, addr, worker_limit=None, *args, **kwargs):
        self.addr = addr
        self.worker_limit = worker_limit
        self.image = None
        self.workers = []
        self.other_args = args
        self.other_kwargs = kwargs

    def connect(self):
        pass

    @property
    def free_slot_number(self):
        return self.worker_limit - len(self.workers)

    def install_image(self, image_name=None, local_path=None, update=True):
        raise NotImplementedError

    def uninstall_image(self):
        raise NotImplementedError

    def start_workers(self, entry_points, num=None):
        raise NotImplementedError

    def clean(self):
        raise NotImplementedError


class DockerViaSSH(BaseService):
    def __init__(self, *args, **kwargs):
        super(DockerViaSSH, self).__init__(*args, **kwargs)
        self.ssh_client = None
        self.dockers = []

    def connect(self):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            self.addr, *self.other_args, **self.other_kwargs)
        if not self.worker_limit:
            self.worker_limit = int(self.ssh_exec("nproc"))

    def ssh_exec(self, cmd):
        logging.info("[EXEC]%s: %s", self.addr, cmd)
        _, out, _ = self.ssh_client.exec_command(cmd)
        if out.channel.recv_exit_status() == 0:
            out = out.read().decode()
            logging.info("[ERET]%s: %s", self.addr, out)
            return out
        else:
            logging.critical("[EXEC]%s: %s", self.addr, cmd)

    def install_image(self, image_name=None, local_path=None, update=True):
        if self.image_exists(image_name):
            if update:
                self.kill_dockers(self.existing_dockers(image=image_name))
                self.uninstall_image(image_name)
            else:
                self.image = image_name
                return
        if local_path:
            sftp = paramiko.SFTPClient.from_transport(
                self.ssh_client.get_transport())
            file_name = os.path.basename(local_path)
            sftp.put(local_path, os.path.join("", file_name))
            out = self.ssh_exec(
                DOCKER_INSTALL_IMAGE_CMD.format(image=file_name))
            for line in out.splitlines():
                if line.startswith("Loaded image:"):
                    self.image = line[13:].strip()
        else:
            self.image = image_name

    def uninstall_image(self, image=None):
        self.ssh_exec(DOCKER_UNINSTALLL_IMAGE_CMD.format(
            image=image or self.image))
        self.image = None

    def image_exists(self, name):
        for line in self.ssh_exec(DOCKER_IMAGES).splitlines():
            image = json.loads(line)
            if name == image["Repository"] or name == image["Repository"]:
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
        self.ssh_exec(DOCKER_RM_CMD.format(
            name=" ".join(dockers or self.dockers)))

    def start_docker(self, entry_points, out_port):
        self.kill_dockers(self.existing_dockers(image=self.image))
        port = engine.DEFAULT_PORT
        name = "mrkt_{}".format(out_port)
        engine_start_cmd = ENGINE_RUN_CMD.format(
            in_port=port, entry_points=entry_points)
        docker_start_cmd = DOCKER_RUN_CMD.format(
            name=name, image=self.image, engine_start_cmd=engine_start_cmd,
            in_port=port, out_port=out_port)
        if self.ssh_exec(docker_start_cmd) != None:
            return name

    def start_workers(self, entry_points, num=math.inf):
        if not isinstance(entry_points, str):
            entry_points = engine.qualified_name(entry_points)
        num = min(self.free_slot_number, num)
        self.dockers = [self.start_docker(entry_points, engine.DEFAULT_PORT)]
        self.workers = [engine.Controller(
            (self.addr, engine.DEFAULT_PORT)) for _ in range(num)]
        return self.workers

    def clean(self, uninstall_image=True):
        self.kill_dockers()
        self.workers = []
        if uninstall_image:
            self.uninstall_image()


class MultiDockerViaSSH(DockerViaSSH):
    def start_workers(self, entry_points, num=math.inf):
        if not isinstance(entry_points, str):
            entry_points = engine.qualified_name(entry_points)
        num = min(self.free_slot_number, num)
        while len(self.workers) < num:
            out_port = engine.DEFAULT_PORT + len(self.dockers)
            self.dockers.append(self.start_docker(entry_points, out_port))
            self.workers.append(engine.Controller((self.addr, out_port)))
        return self.workers
