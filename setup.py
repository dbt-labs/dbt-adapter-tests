from setuptools import setup


setup(
    name='pytest-dbt-adapter',
    modules=['pytest_dbt_adapter'],
    version='0.1',
    package_data={
        'pytest_dbt_adapter': [
            'basic_crud_tests/dbt_project.yml',
            'basic_crud_tests/data/*.csv',
            'basic_crud_tests/macros/*.sql',
            'basic_crud_tests/models/*/*.sql',
            'basic_crud_tests/models/*.yml',
            'empty_project/dbt_project.yml',
        ]
    },
    entry_points={
        'pytest11': [
            'pytest_dbt_adapter = pytest_dbt_adapter',
        ]
    },
    install_requires=['py>=1.3.0', 'pyyaml'],
)
