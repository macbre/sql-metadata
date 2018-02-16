from setuptools import setup

VERSION = '1.1.1'

# @see https://github.com/pypa/sampleproject/blob/master/setup.py
setup(
    name='sql_metadata',
    version=VERSION,
    author='Maciej Brencz',
    author_email='maciej.brencz@gmail.com',
    license='MIT',
    description='Uses tokenized query returned by python-sqlparse and generates query metadata',
    url='https://github.com/macbre/sql-metadata',
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Database',

        # Pick your license as you wish
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    py_modules=["sql_metadata"],
    install_requires=[
        'coverage==4.4.2',
        'pylint==1.8.1',
        'pytest>=3.2.3',
        'sqlparse==0.2.4',
    ]
)
