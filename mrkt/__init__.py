try:
    from .cluster import Cluster
    from .service import SSHService as SSH
except ModuleNotFoundError:
    pass
