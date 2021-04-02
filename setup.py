import re
from pathlib import Path

from setuptools import setup, find_packages

install_requires = ["psycopg2-binary>=2.8.4", "async_timeout>=3.0,<4.0"]
extras_require = {"sa": ["sqlalchemy[postgresql_psycopg2binary]>=1.3,<1.5"]}


def read(*parts):
    return Path(__file__).resolve().parent.joinpath(*parts).read_text().strip()


def get_maintainers(path="MAINTAINERS.txt"):
    return ", ".join(x.strip().strip("*").strip() for x in read(path).splitlines())


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*\"([\d.abrc]+)\"")
    for line in read("aiopg", "__init__.py").splitlines():
        match = regexp.match(line)
        if match is not None:
            return match.group(1)

    raise RuntimeError("Cannot find version in aiopg/__init__.py")


def read_changelog(path="CHANGES.txt"):
    return f"Changelog\n---------\n\n{read(path)}"


classifiers = [
    "License :: OSI Approved :: BSD License",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3 :: Only",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Operating System :: POSIX",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
    "Environment :: Web Environment",
    "Development Status :: 5 - Production/Stable",
    "Topic :: Database",
    "Topic :: Database :: Front-Ends",
    "Framework :: AsyncIO",
]

setup(
    name="aiopg",
    version=read_version(),
    description="Postgres integration with asyncio.",
    long_description="\n\n".join((read("README.rst"), read_changelog())),
    long_description_content_type="text/x-rst",
    classifiers=classifiers,
    platforms=["macOS", "POSIX", "Windows"],
    author="Andrew Svetlov",
    python_requires=">=3.6",
    project_urls={
        "Chat: Gitter": "https://gitter.im/aio-libs/Lobby",
        "CI: GA": "https://github.com/aio-libs/aiopg/actions?query=workflow%3ACI",
        "Coverage: codecov": "https://codecov.io/gh/aio-libs/aiopg",
        "Docs: RTD": "https://aiopg.readthedocs.io",
        "GitHub: issues": "https://github.com/aio-libs/aiopg/issues",
        "GitHub: repo": "https://github.com/aio-libs/aiopg",
    },
    author_email="andrew.svetlov@gmail.com",
    maintainer=get_maintainers(),
    maintainer_email="virmir49@gmail.com",
    url="https://aiopg.readthedocs.io",
    download_url="https://pypi.python.org/pypi/aiopg",
    license="BSD",
    packages=find_packages(),
    package_data={"aiopg": ["py.typed"]},
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True,
)
