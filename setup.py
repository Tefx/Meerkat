from setuptools import setup

setup(name="mrkt",
      version="0.1",
      author="tefx",
      packages=["mrkt"],
      include_package_data=True,
      install_requires=["gevent", "paramiko", "boto3"],
      entry_points=dict(
          console_scripts=["mrkt-ng=mrkt.engine:run_engine",
                           "mrkt-gdf=mrkt.engine:gen_dockerfile"]))
