#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'Click>=7.0', 
    'parsl>=1.2.0',
    'kubernetes>=21.7.0'
]

test_requirements = ['pytest>=3', ]

setup(
    author="Matthew B. Jones",
    author_email='jones@nceas.ucsb.edu',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    description="PDG Visualization workflow using Parsl.",
    entry_points={
        'console_scripts': [
            'pdg_workflow=pdg_workflow.cli:main',
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='pdg_workflow',
    name='pdg_workflow',
    packages=find_packages(include=['pdg_workflow', 'pdg_workflow.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/PermafrostDiscoveryGateway/viz-workflow',
    version='0.1.0',
    zip_safe=False,
)
