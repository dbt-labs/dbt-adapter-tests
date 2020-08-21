from setuptools import setup


setup(
    name='pytest-dbt-adapter',
    packages=['pytest_dbt_adapter'],
    version='0.1',
    package_data={
        'pytest_dbt_adapter': [
            'projects/*.yml',
            'sequences/*.yml',
        ]
    },
    entry_points={
        'pytest11': [
            'pytest_dbt_adapter = pytest_dbt_adapter',
        ]
    },
    install_requires=['py>=1.3.0', 'pyyaml'],
)
