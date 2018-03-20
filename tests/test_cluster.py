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

    from mrkt.framework import Pool
    from mrkt.framework.platform.local import Hosts
    from mrkt.framework.platform.AWS import EC2
    from mrkt.framework.service.docker import ViaSSH

    platforms = [
        Hosts(ViaSSH("localhost")),
        # DirectSSH("192.168.0.100", ssh_options=dict(username="tefx")),
        # EC2(requests={"c4.2xlarge": 4},
        #     sgroup="sg-c86bc4ae",
        #     key_name="research",
        #     key_file="../../research.pem",
        #     clean_action=EC2.CleanAction.Stop),
    ]

    with Pool(platforms, image="tefx/mrkt:alpine", image_update=False) as pool:
        print(sum(pool.map(sqr, range(10))))
