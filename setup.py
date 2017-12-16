from setuptools import setup

setup(name="mrkt",
      version="0.1",
      author="tefx",
      packages=["mrkt", "mrkt.platform"],
      include_package_data=True,
      install_requires=["gevent"],
      extras_require={
          'ssh': ["paramiko"],
          'AWS': ["boto3"],
      },
      entry_points=dict(
          console_scripts=["mrkt-agent=mrkt.agent:run_agent",
                           "mrkt-pack=mrkt.tools:pack_docker"]))
