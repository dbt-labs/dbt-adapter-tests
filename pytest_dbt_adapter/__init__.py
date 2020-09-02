from .spec_file import DbtSpecFile


# custom test loader
def pytest_collect_file(parent, path):
    if path.ext == ".dbtspec":  # and path.basename.startswith("test"):
        return DbtSpecFile.from_parent(parent, fspath=path)


def pytest_addoption(parser):
    group = parser.getgroup('dbtadapter')
    group.addoption(
        '--no-drop-schema',
        action='store_false',
        dest='drop_schema',

    )
