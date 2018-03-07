from time import sleep


def sqr(x):
    sleep(1)
    return x ** 2


if __name__ == '__main__':
    import logging
    import sys
    import os

    logging.getLogger("mrkt").setLevel(logging.INFO)
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

    from mrkt.framework import Cluster, Host
    from mrkt.framework.platform import Local, EC2

    platforms = [
        Local(Host("localhost")),
        # DirectSSH("192.168.0.100", ssh_options=dict(username="tefx")),
        # EC2(srvc_dict={"c4.2xlarge": 1},
        #     sgroup="sg-c86bc4ae",
        #     keyname="research",
        #     keyfile="../../research.pem",
        #     clean_action=EC2.CleanAction.Stop),
    ]

    with Cluster(platforms) as cluster:
        print(sum(cluster.map(sqr, range(50))))
