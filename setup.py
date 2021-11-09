#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGES.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=7.0', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Casper van der Wel",
    author_email='casper.vanderwel@nelen-schuurmans.nl',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    description="Tooling to migrate models from models.lizard.net to 3di.live",
    entry_points={
        'console_scripts': [
            'threedi_model_migration=threedi_model_migration.cli:main',
        ],
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='threedi_model_migration',
    name='threedi_model_migration',
    packages=find_packages(include=['threedi_model_migration', 'threedi_model_migration.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/caspervdw/threedi_model_migration',
    version='0.1.0',
    zip_safe=False,
)
