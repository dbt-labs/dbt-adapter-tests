from setuptools import setup


def read(path):
    with open(path, encoding='utf-8') as fp:
        return fp.read()


setup(
    name='pytest-dbt-adapter',
    packages=['pytest_dbt_adapter'],
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-adapter-tests",
    version='0.6.0',
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
    install_requires=['py>=1.3.0', 'pytest>=6', 'pyyaml>=3.11'],
    description="A pytest plugin for testing dbt adapter plugins",
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    python_requires=">=3.6.2",
)
