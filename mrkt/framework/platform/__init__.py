from .local import Local

try:
    from .ec2 import EC2
except ModuleNotFoundError:
    pass