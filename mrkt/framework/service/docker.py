import os
import os.path
import paramiko
import json
from gevent import sleep
from logging import getLogger

from ..role import Service
from ...common.utils import listify
from ...common.consts import *

logger = getLogger(__name__)

CMD_AGENT_START = "mrkt-agent -p {in_port} -l info ."
CMD_DOCKER_START_CONTAINER = "docker run -itd --name {name} -p {out_port}:{in_port} {image} {engine_start_cmd}"
CMD_DOCKER_RM_CONTAINER = "docker rm -f {name}"
CMD_DOCKER_INSTALL_IMAGE = "gunzip -c {image} | docker load && rm {image}"
CMD_DOCKER_UPDATE_IMAGE = "docker pull {image}"
CMD_DOCKER_UNINSTALLL_IMAGE = "docker rmi {image}"
CMD_DOCKER_LS_CONTAINERS = "docker container ls -a --format \"{{json .Names}}\""
CMD_DOCKER_LS_IMAGES = "docker images --format \"{{json .Repository}}\""
CMD_DOCKER_LIST_NONE_IMAGES = "docker images | grep '<none>' | awk '{print $3}'"


class ViaSSH(Service):
    def __init__(self, address, **ssh_options):
        super(ViaSSH, self).__init__(address)
        self.ssh_client = None
        self.ssh_options = ssh_options
        self.retry_ssh = SERVICE_SSH_RETRY_TIMES
        self.retry_ssh_interval = SERVICE_SSH_RETRY_INTERVAL

    def set_options(self, *options_list):
        ssh_options = self.ssh_options
        super(ViaSSH, self).set_options(*options_list)
        self.ssh_options.update(ssh_options)

    def connect(self):
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        last_exception = Exception("SSH retry times < 0")
        for times in range(self.retry_ssh):
            logger.debug("[%s.connect] to %s with %s, %s/%s times",
                         self.__class__.__name__,
                         self.address, self.ssh_options,
                         times + 1, self.retry_ssh)
            try:
                ssh_client.connect(self.address, **self.ssh_options)
                logger.info("[%s.connect] to %s succeed",
                            self.__class__.__name__,
                            self.address)
                self.ssh_client = ssh_client
                return
            except paramiko.ssh_exception.NoValidConnectionsError as xcp:
                last_exception = xcp
            sleep(self.retry_ssh_interval)
        raise last_exception

    @listify()
    def existing_images(self, only_outdated=False):
        if only_outdated:
            for line in self.cmd(CMD_DOCKER_LIST_NONE_IMAGES).splitlines():
                yield line.strip()
        else:
            for line in self.cmd(CMD_DOCKER_LS_IMAGES).splitlines():
                yield json.loads(line)

    def install_image_via_archive(self, archive):
        file_name = os.path.basename(archive)
        sftp = paramiko.SFTPClient.from_transport(
            self.ssh_client.get_transport())
        sftp.put(archive, os.path.join("", file_name))
        out = self.cmd(
            CMD_DOCKER_INSTALL_IMAGE.format(image=file_name))
        for line in out.splitlines():
            if line.startswith("Loaded image:"):
                return line[13:].strip()

    def install_image_via_name(self, image_name):
        self.cmd(CMD_DOCKER_UPDATE_IMAGE.format(image=image_name))
        return image_name

    def uninstall_images(self, images):
        if images:
            self.cmd(CMD_DOCKER_UNINSTALLL_IMAGE.format(
                image=" ".join(images)))

    @listify()
    def existing_containers(self):
        for line in self.cmd(CMD_DOCKER_LS_CONTAINERS).splitlines():
            yield json.loads(line)

    def start_containers(self, out_port):
        name = "{}_{}".format(SERVICE_CONTAINER_PREFIX, out_port)
        engine_start_cmd = CMD_AGENT_START.format(in_port=AGENT_PORT)
        docker_start_cmd = CMD_DOCKER_START_CONTAINER.format(
            name=name, image=self.image, engine_start_cmd=engine_start_cmd,
            in_port=AGENT_PORT, out_port=out_port)
        return name if self.cmd(docker_start_cmd) else None

    def kill_containers(self, dockers=None):
        dockers = dockers or self.containers
        if dockers:
            try:
                self.cmd("uname")
            except ConnectionResetError:
                self.connect()
            self.cmd(CMD_DOCKER_RM_CONTAINER.format(name=" ".join(dockers)))

    def clean(self):
        super(ViaSSH, self).clean()
        if self.ssh_client:
            self.ssh_client.close()
            self.ssh_client = None

    def cmd(self, cmd):
        logger.info("[%s.cmd] on %s: %s",
                    self.__class__.__name__,
                    self.address, cmd)
        _, out, err = self.ssh_client.exec_command(cmd)
        if out.channel.recv_exit_status() != 0:
            logger.critical("[%s.cmd] on %s: %s",
                            self.__class__.__name__,
                            self.address, cmd)
            logger.critical("[%s.cmd] on %s: %s",
                            self.__class__.__name__,
                            self.address, err.read().decode())
            return None
        out = out.read().decode()
        logger.debug("[%s.ssh_exec] on %s: out: %s", self.address, out)
        return out
