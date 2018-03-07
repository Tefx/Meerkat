from time import sleep


def sqr(x):
    sleep(1)
    return x ** 2


if __name__ == '__main__':
    import logging
    import sys
    import os

    # logging.basicConfig(level=logging.INFO)
    logging.getLogger("mrkt").setLevel(logging.INFO)
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from mrkt.cluster import Cluster
    from mrkt.platform import Local, EC2
    from mrkt.service import SSHService as SSH

    platforms = [
        # Local(SSH("localhost")),
        # DirectSSH("192.168.0.100", ssh_options=dict(username="tefx")),
        EC2(srvc_dict={"c4.2xlarge": 1},
            sgroup="sg-c86bc4ae",
            keyname="research",
            keyfile="../../research.pem",
            clean_action="none"),
    ]

    with Cluster(platforms, image="tefx/mrkt", image_update=True, image_clean=False) as cluster:
        cluster.sync_dir(".")
        res = list(cluster.map(sqr, range(100)))
        print(sum(res))
