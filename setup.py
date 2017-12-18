from setuptools import setup

setup(name="mrkt",
      version="0.1",
      author="tefx",
      packages=["mrkt", "mrkt.platform", "mrkt.agent"],
      include_package_data=True,
      install_requires=["gevent", "dill"],
      extras_require={
          'ssh': ["paramiko"],
          'AWS': ["boto3"],
      },
      entry_points=dict(
          console_scripts=["mrkt-agent=mrkt.agent.dynamic:DynamicAgent.launch",
                           "mrkt-pack=mrkt.tools:pack_docker"]))
