import os
import re

from setuptools import setup, find_packages

install_requires = ['psycopg2-binary>=2.8.4', 'async_timeout>=3.0,<4.0']
extras_require = {'sa': ['sqlalchemy[postgresql_psycopg2binary]>=1.1']}


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


def get_maintainers(path='MAINTAINERS.txt'):
    with open(os.path.join(os.path.dirname(__file__), path)) as f:
        return ', '.join(x.strip().strip('*').strip() for x in f.readlines())


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__), 'aiopg', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in aiopg/__init__.py')


def read_changelog(path='CHANGES.txt'):
    return 'Changelog\n---------\n\n{}'.format(read(path))


classifiers = [
    'License :: OSI Approved :: BSD License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3 :: Only',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Operating System :: POSIX',
    'Operating System :: MacOS :: MacOS X',
    'Operating System :: Microsoft :: Windows',
    'Environment :: Web Environment',
    'Development Status :: 5 - Production/Stable',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Framework :: AsyncIO',
]

setup(
    name='aiopg',
    version=read_version(),
    description='Postgres integration with asyncio.',
    long_description='\n\n'.join((read('README.rst'), read_changelog())),
    long_description_content_type='text/x-rst',
    classifiers=classifiers,
    platforms=['macOS', 'POSIX', 'Windows'],
    author='Andrew Svetlov',
    python_requires='>=3.6',
    project_urls={
        'Chat: Gitter': 'https://gitter.im/aio-libs/Lobby',
        'CI: GA': 'https://github.com/aio-libs/aiopg/actions?query=workflow%3ACI',
        'Coverage: codecov': 'https://codecov.io/gh/aio-libs/aiopg',
        'Docs: RTD': 'https://aiopg.readthedocs.io',
        'GitHub: issues': 'https://github.com/aio-libs/aiopg/issues',
        'GitHub: repo': 'https://github.com/aio-libs/aiopg',
    },
    author_email='andrew.svetlov@gmail.com',
    maintainer=get_maintainers(),
    maintainer_email='virmir49@gmail.com',
    url='https://aiopg.readthedocs.io',
    download_url='https://pypi.python.org/pypi/aiopg',
    license='BSD',
    packages=find_packages(),
    install_requires=install_requires,
    extras_require=extras_require,
    include_package_data=True
)
