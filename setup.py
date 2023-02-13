from setuptools import setup, find_packages

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="thetadatawrapper",
    version="0.1",
    description='A wrapper on top of thetadata api',
    author='Alex Hubert',
    author_email='your.email@example.com',
    packages=find_packages(),
    install_requires=required,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
)
