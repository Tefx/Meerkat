# meerkat
Using Dockers to parallelize python programs

# Example

```
from mrkt.cluster import Cluster
from mrkt.platform.AWS import EC2
from mrkt.service import DockerViaSSH


def double(x):
    return x * 2


if __name__ == '__main__':
    services = [
        EC2(DockerViaSSH, 1,
            sgroup="sg-c86bc4ae",
            keyname="research",
            keyfile="../research.pem",
            clean_action="terminate"),
        DockerViaSSH("localhost"),
        DockerViaSSH("192.168.0.199", ssh_options=dict(username="tefx"))
    ]

    with Cluster(services, image="test") as cluster:
        print(cluster.map(double, range(20)))
```
