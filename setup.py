from setuptools import setup


def read(path):
    with open(path, encoding='utf-8') as fp:
        return fp.read()


setup(
    name='pytest-dbt-adapter',
    packages=['pytest_dbt_adapter'],
    version='0.1.0',
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
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
)
