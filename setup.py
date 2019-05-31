from setuptools import find_packages, setup
from akfak_kp import __version__ as akfak_version

requirements = [l.split(' ')[0] for l in open('requirements.txt').read().splitlines() if not l.startswith('#')]

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
        akfak-kp=akfak_kp.cli:cli
    """
)
