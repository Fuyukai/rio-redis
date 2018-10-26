import sys
from pathlib import Path

from setuptools import setup

if sys.version_info[0:2] < (3, 6):
    raise RuntimeError("This package requires Python 3.6+.")

setup(
    name="rio-redis",
    use_scm_version={
        "version_scheme": "guess-next-dev",
        "local_scheme": "dirty-tag"
    },
    packages=[
        "rioredis"
    ],
    url="https://github.com/rio-libs/rio-redis",
    license="LGPLv3",
    author="Laura Dickinson",
    author_email="l@veriny.tf",
    description="A multio library for Redis",
    long_description=Path(__file__).with_name("README.rst").read_text(encoding="utf-8"),
    setup_requires=[
        "setuptools_scm",
    ],
    install_requires=[
        "hiredis",
        "anyio",
    ],
    extras_require={
        "tests": [
            "pytest",
            "pytest-cov",
            "trio>=0.5.0",
            "curio",
            "codecov",
        ]
    },
    test_requires=[],
    python_requires=">=3.6.0",
)
