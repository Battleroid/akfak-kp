from pip.download import PipSession
from pip.req import parse_requirements
from setuptools import find_packages, setup

from akfak import __version__ as akfak_version

reqs = parse_requirements('requirements.txt', session=PipSession())
requirements = [str(req.req) for req in reqs]

setup(
    name='akfak-kp',
    author='Casey Weed',
    author_email='cweed@caseyweed.com.com',
    description='kafka-python version of Akfak',
    url='https://github.com/battleroid/akfak-kp',
    version=akfak_version,
    packages=find_packages(),
    install_requires=requirements,
    entry_points="""
        [console_scripts]
        akfak-kp=akfak.cli:cli
    """
)
