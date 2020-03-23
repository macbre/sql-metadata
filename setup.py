from setuptools import setup

VERSION = '1.6.0'

# @see https://packaging.python.org/tutorials/packaging-projects/#creating-setup-py
with open("README.md", "r") as fh:
    long_description = fh.read()

# @see https://github.com/pypa/sampleproject/blob/master/setup.py
setup(
    name='sql_metadata',
    version=VERSION,
    author='Maciej Brencz',
    author_email='maciej.brencz@gmail.com',
    license='MIT',
    description='Uses tokenized query returned by python-sqlparse and generates query metadata',
    long_description=long_description,
    long_description_content_type="text/markdown",
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

        # Specify the Python versions you support here.
        'Programming Language :: Python :: 3',
    ],
    py_modules=["sql_metadata"],
    extras_require={
        'dev': [
            'coverage==5.0.4',
            'pylint==2.4.4',
            'pytest==5.4.1',
        ]
    },
    install_requires=[
        'sqlparse==0.3.1',
    ]
)
