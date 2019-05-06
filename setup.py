from setuptools import setup, find_packages
from waterfall import __version__


setup(
    name="Waterfall",
    version=__version__,
    author="Justin L. MacCallum",
    author_email="justin.maccallum@ucalgary.ca",
    packages=find_packages(),
    url="http://https://github.com/maccallumlab/waterfall",
    license="LICENSE.txt",
    description="Waterfall Sampling",
    long_description=open("README.md").read(),
)
