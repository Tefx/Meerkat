from setuptools import setup, find_packages

setup(name="mrkt",
      version="0.1",
      author="tefx",
      packages=find_packages(),
      include_package_data=True,
      install_requires=["gevent", "dill", "lz4"],
      extras_require={
          "SSH": ["paramiko"],
          'AWS': ["boto3"],
      },
      entry_points=dict(
          console_scripts=["mrkt-agent=mrkt.agent:DynamicAgent.launch",
                           "mrkt-pack=mrkt.tools:pack_docker"]))
