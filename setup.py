import os
import re
import sys
from setuptools import setup


install_requires = ['psycopg2-binary>=2.7.0']
extras_require = {'sa': ['sqlalchemy[postgresql_psycopg2binary]>=1.1']}

PY_VER = sys.version_info

if PY_VER < (3, 4):
    raise RuntimeError("aiopg doesn't support Python earlier than 3.4")


def read(f):
    return open(os.path.join(os.path.dirname(__file__), f)).read().strip()


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


classifiers = [
    'License :: OSI Approved :: BSD License',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Operating System :: POSIX',
    'Operating System :: MacOS :: MacOS X',
    'Operating System :: Microsoft :: Windows',
    'Environment :: Web Environment',
    'Development Status :: 4 - Beta',
    'Topic :: Database',
    'Topic :: Database :: Front-Ends',
    'Framework :: AsyncIO',
]


setup(name='aiopg',
      version=read_version(),
      description='Postgres integration with asyncio.',
      long_description='\n\n'.join((read('README.rst'), read('CHANGES.txt'))),
      classifiers=classifiers,
      platforms=['POSIX'],
      author='Andrew Svetlov',
      author_email='andrew.svetlov@gmail.com',
      url='https://aiopg.readthedocs.io',
      download_url='https://pypi.python.org/pypi/aiopg',
      license='BSD',
      packages=['aiopg', 'aiopg.sa'],
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data=True)
