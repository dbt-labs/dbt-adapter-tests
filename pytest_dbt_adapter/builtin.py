import os
from pathlib import Path
from typing import Dict, Any

import yaml

from .exceptions import TestProcessingException


DEFAULT_DBT_PROJECT = {
    'name': 'dbt_test_project',
    'config-version': 2,
    'version': '1.0.0',
}


NAMES_BASE = """
id,name,some_date
1,Easton,1981-05-20T06:46:51
2,Lillian,1978-09-03T18:10:33
3,Jeremiah,1982-03-11T03:59:51
4,Nolan,1976-05-06T20:21:35
5,Hannah,1982-06-23T05:41:26
6,Eleanor,1991-08-10T23:12:21
7,Lily,1971-03-29T14:58:02
8,Jonathan,1988-02-26T02:55:24
9,Adrian,1994-02-09T13:14:23
10,Nora,1976-03-01T16:51:39
""".lstrip()


NAMES_EXTENDED = NAMES_BASE + """
11,Mateo,2014-09-07T17:04:27
12,Julian,2000-02-04T11:48:30
13,Gabriel,2001-07-10T07:32:52
14,Isaac,2002-11-24T03:22:28
15,Levi,2009-11-15T11:57:15
16,Elizabeth,2005-04-09T03:50:11
17,Grayson,2019-08-06T19:28:17
18,Dylan,2014-03-01T11:50:41
19,Jayden,2009-06-06T07:12:49
20,Luke,2003-12-05T21:42:18
""".lstrip()


NAMES_ADD_COLUMN = """
id,name,some_date,last_initial
1,Easton,1981-05-20T06:46:51,A
2,Lillian,1978-09-03T18:10:33,B
3,Jeremiah,1982-03-11T03:59:51,C
4,Nolan,1976-05-06T20:21:35,D
5,Hannah,1982-06-23T05:41:26,E
6,Eleanor,1991-08-10T23:12:21,F
7,Lily,1971-03-29T14:58:02,G
8,Jonathan,1988-02-26T02:55:24,H
9,Adrian,1994-02-09T13:14:23,I
10,Nora,1976-03-01T16:51:39,J
""".lstrip()


class Model:
    def __init__(self, config, body):
        self.config = config
        self.body = body

    @classmethod
    def from_dict(cls, dct):
        try:
            config = dct.get('config', {})
            if 'materialized' in dct:
                config['materialized'] = dct['materialized']
            return cls(config=config, body=dct['body'])
        except KeyError as exc:
            raise TestProcessingException(
                f'Invalid test, model is missing key {exc}'
            )

    def config_params(self):
        if not self.config:
            return ''
        else:
            pairs = ', '.join(
                '{!s}={!r}'.format(key, value)
                for key, value in self.config.items()
            )
            return '{{ config(' + pairs + ') }}'

    def render(self):
        return '\n'.join([self.config_params(), self.body])


class DbtProject:
    def __init__(
        self,
        name: str,
        dbt_project_yml: str,
        paths: Dict[str, str],
        facts: Dict[str, Any]
    ):
        self.name = name
        self.dbt_project_yml = dbt_project_yml
        self.paths = paths
        self.facts = facts

    def write(self, path: str):
        project_path = os.path.join(path, 'project')
        os.makedirs(project_path)
        with open(os.path.join(project_path, 'dbt_project.yml'), 'w') as fp:
            fp.write(yaml.safe_dump(self.dbt_project_yml))

        for relpath, contents in self.paths.items():
            fullpath = os.path.join(project_path, relpath)
            os.makedirs(os.path.dirname(fullpath), exist_ok=True)

            if isinstance(contents, str) and contents.startswith('files.'):
                contents_path = contents.split('.')[1:]
                cur = KNOWN_FILES
                for part in contents_path:
                    if part not in cur:
                        raise TestProcessingException(
                            f'at known file lookup {contents}, could not find '
                            f'part {part} in known files path'
                        )
                    cur = cur[part]
                contents = cur

            if relpath.startswith('models/') and relpath.endswith('.sql'):
                if isinstance(contents, dict):
                    model = Model.from_dict(contents)
                    contents = model.render()
            if not isinstance(contents, str):
                raise TestProcessingException(f'{contents} is not a string')

            with open(fullpath, 'w') as fp:
                fp.write(contents)

    @classmethod
    def from_dict(cls, dct, overriding=None):
        if overriding is None:
            overriding = {}

        paths: Dict[str, Any]
        facts: Dict[str, Any]
        dbt_project_yml: Dict[str, Any]

        if 'overrides' in dct:
            name = dct['overrides']
            if name not in overriding:
                raise TestProcessingException(
                    f'Invalid project definition, override name {name} not '
                    'known'
                ) from None
            dbt_project_yml = overriding[name].dbt_project_yml.copy()
            paths = overriding[name].paths.copy()
            facts = overriding[name].facts.copy()
        else:
            try:
                name = dct['name']
            except KeyError:
                raise TestProcessingException(
                    f'Invalid project definition, no name in {dct}'
                ) from None

            dbt_project_yml = DEFAULT_DBT_PROJECT.copy()
            paths = {}
            facts = {}

        dbt_project_yml.update(dct.get('dbt_project_yml', {}))

        paths.update(dct.get('paths', {}))
        facts.update(dct.get('facts', {}))
        return cls(
            name=name,
            dbt_project_yml=dbt_project_yml,
            paths=paths,
            facts=facts,
        )


SEED_SOURCE_YML = """
version: 2
sources:
  - name: raw
    schema: "{{ target.schema }}"
    tables:
      - name: seed
        identifier: "{{ var('seed_name', 'base') }}"
"""

TEST_SEEDS_SCHEMA_YML_TEST_BASE = """
version: 2
models:
  - name: base
    columns:
     - name: id
       tests:
         - not_null
"""

TEST_MODELS_SCHEMA_YML_TEST_VIEW = """
version: 2
models:
  - name: view
    columns:
     - name: id
       tests:
         - not_null
"""

TEST_MODELS_SCHEMA_YML_TEST_TABLE = """
version: 2
models:
  - name: table
    columns:
     - name: id
       tests:
         - not_null
"""


TEST_PASSING_DATA_TEST = """
select 1 as id where id = 2
"""

TEST_FAILING_DATA_TEST = """
select 1 as id where id = 1
"""


TEST_EPHEMERAL_DATA_TEST_PASSING = '''
with my_other_cool_cte as (
    select id, name from {{ ref('ephemeral') }}
    where id > 1000
)
select name, id from my_other_cool_cte
'''


TEST_EPHEMERAL_DATA_TEST_FAILING = '''
with my_other_cool_cte as (
    select id, name from {{ ref('ephemeral') }}
    where id < 1000
)
select name, id from my_other_cool_cte
'''

INCREMENTAL_MODEL = """
select * from {{ source('raw', 'seed') }}
{% if is_incremental() %}
where id > (select max(id) from {{ this }})
{% endif %}
""".strip()


CC_ALL_SNAPSHOT_SQL = '''
{% snapshot cc_all_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
'''.strip()


CC_NAME_SNAPSHOT_SQL = '''
{% snapshot cc_name_snapshot %}
    {{ config(
        check_cols=['name'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
'''.strip()


CC_DATE_SNAPSHOT_SQL = '''
{% snapshot cc_date_snapshot %}
    {{ config(
        check_cols=['some_date'], unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
'''.strip()


TS_SNAPSHOT_SQL = '''
{% snapshot ts_snapshot %}
    {{ config(
        strategy='timestamp',
        unique_key='id',
        updated_at='some_date',
        target_database=database,
        target_schema=schema,
    )}}
    select * from {{ ref(var('seed_name', 'base')) }}
{% endsnapshot %}
'''.strip()


EPHEMERAL_WITH_CTE = """
with my_cool_cte as (
  select name, id from {{ ref('base') }}
)
select id, name from my_cool_cte where id is not null
"""


KNOWN_FILES = {
    'seeds': {
        'base': NAMES_BASE,
        'newcolumns': NAMES_ADD_COLUMN,
        'added': NAMES_EXTENDED,
    },
    'models': {
        'base_materialized_var': """
            {{ config(materialized=var("materialized_var", "table"))}}
            select * from {{ source('raw', 'seed') }}
        """,
        'base_table': {
            'materialized': 'table',
            'body': "select * from {{ source('raw', 'seed') }}",
        },
        'base_view': {
            'materialized': 'view',
            'body': "select * from {{ source('raw', 'seed') }}",
        },
        'ephemeral': {
            'materialized': 'ephemeral',
            'body': "select * from {{ source('raw', 'seed') }}",
        },
        'ephemeral_with_cte': {
            'materialized': 'ephemeral',
            'body': EPHEMERAL_WITH_CTE,
        },
        'ephemeral_view': {
            'materialized': 'view',
            'body': "select * from {{ ref('ephemeral') }}",
        },
        'ephemeral_table': {
            'materialized': 'table',
            'body': "select * from {{ ref('ephemeral') }}",
        },
        'incremental': {
            'materialized': 'incremental',
            'body': INCREMENTAL_MODEL,
        }
    },
    'snapshots': {
        'check_cols_all': CC_ALL_SNAPSHOT_SQL,
        'check_cols_name': CC_NAME_SNAPSHOT_SQL,
        'check_cols_date': CC_DATE_SNAPSHOT_SQL,
        'timestamp': TS_SNAPSHOT_SQL,
    },
    'tests': {
        'passing': TEST_PASSING_DATA_TEST,
        'failing': TEST_FAILING_DATA_TEST,
        'ephemeral': {
            'passing': TEST_EPHEMERAL_DATA_TEST_PASSING,
            'failing': TEST_EPHEMERAL_DATA_TEST_FAILING,
        }
    },
    'schemas': {
        'base': SEED_SOURCE_YML,
        'test_seed': TEST_SEEDS_SCHEMA_YML_TEST_BASE,
        'test_view': TEST_MODELS_SCHEMA_YML_TEST_VIEW,
        'test_table': TEST_MODELS_SCHEMA_YML_TEST_TABLE,
    },
}


THIS_DIR = Path(__file__).parent
PROJECT_DIR = THIS_DIR / 'projects'
SEQUENCE_DIR = THIS_DIR / 'sequences'


def _get_named_yaml_dicts(path: Path) -> Dict[str, Dict[str, Any]]:
    result = {}
    for project_path in path.glob('**/*.yml'):
        try:
            data = yaml.safe_load(project_path.read_text())
            name = data['name']
        except KeyError as exc:
            raise ImportError(
                f'Invalid project file at {project_path}: no name'
            ) from exc
        except Exception as exc:
            raise ImportError(
                f'Could not read project file at {project_path}: {exc}'
            ) from exc
        result[name] = data
    return result


DEFAULT_PROJECTS = _get_named_yaml_dicts(PROJECT_DIR)

BUILTIN_TEST_SEQUENCES = _get_named_yaml_dicts(SEQUENCE_DIR)
