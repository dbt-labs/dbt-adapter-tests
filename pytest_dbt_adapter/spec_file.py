import json
import os
import random
import shlex
import tempfile
from datetime import datetime
from itertools import chain, repeat
from subprocess import run, CalledProcessError
from typing import Dict, Any, Iterable

import pytest
import yaml

from .exceptions import TestProcessingException, DBTException
from .builtin import BUILTIN_TEST_SEQUENCES, DEFAULT_PROJECTS, DbtProject


from dbt.adapters.factory import FACTORY
from dbt.config import RuntimeConfig
from dbt.main import parse_args


class DbtSpecFile(pytest.File):
    def collect(self):
        with self.fspath.open() as fp:
            raw = yaml.safe_load(fp)
        if not raw:
            return
        try:
            raw_target = raw['target']
        except KeyError:
            raise TestProcessingException(
                'Invalid dbtspec: target not found'
            ) from None

        projects = {
            k: DbtProject.from_dict(v)
            for k, v in DEFAULT_PROJECTS.items()
        }

        for project in raw.get('projects', []):
            parsed = DbtProject.from_dict(project, projects)
            projects[parsed.name] = parsed

        try:
            sequences = raw['sequences']
        except KeyError:
            raise TestProcessingException(
                'Invalid dbtspec: sequences not found'
            ) from None

        for name, testdef in sequences.items():
            if isinstance(testdef, str):
                try:
                    testdef = BUILTIN_TEST_SEQUENCES[testdef]
                except KeyError:
                    raise TestProcessingException(
                        f'Unknown builtin test name {testdef}'
                    )
            try:
                project_name = testdef['project']
            except KeyError:
                raise TestProcessingException(
                    f'Invalid dbtspec: no project in sequence {testdef}'
                ) from None

            try:
                project = projects[project_name]
            except KeyError:
                raise TestProcessingException(
                    f'Invalid dbtspec: project {project_name} unknown'
                ) from None

            try:
                sequence = testdef['sequence']
            except KeyError:
                raise TestProcessingException(
                    f'Invalid dbtspec: no sequence in sequence {testdef}'
                ) from None

            yield DbtItem.from_parent(
                self,
                name=name,
                target=raw_target,
                sequence=sequence,
                project=project,
            )


class DbtItem(pytest.Item):
    def __init__(self, name, parent, target, sequence, project):
        super().__init__(name, parent)
        self.target = target
        self.sequence = sequence
        self.project = project
        self.adapter = None
        self.schema_relation = None
        start = datetime.utcnow().strftime('%y%m%d%H%M%S%f')
        randval = random.SystemRandom().randint(0, 999999)
        self.random_suffix = f'{start}{randval:06}'

    def _base_vars(self):
        return {'_dbt_random_suffix': self.random_suffix}

    def _get_adapter(self, tmpdir):
        project_path = os.path.join(tmpdir, 'project')
        args = parse_args([
            'compile', '--profile', 'dbt-pytest', '--target', 'default',
            '--project-dir', project_path, '--profiles-dir', tmpdir,
            '--vars', yaml.safe_dump(self._base_vars()),
        ])
        with open(os.path.join(args.profiles_dir, 'profiles.yml')) as fp:
            data = yaml.safe_load(fp)
            try:
                profile = data[args.profile]
            except KeyError:
                raise ValueError(f'profile {args.profile} not found')
            try:
                outputs = profile['outputs']
            except KeyError:
                raise ValueError(f'malformed profile {args.profile}')
            try:
                target = outputs[args.target]
            except KeyError:
                raise ValueError(
                    f'target {args.target} not found in {args.profile}'
                )
            try:
                adapter_type = target['type']
            except KeyError:
                raise ValueError(
                    f'target {args.target} in {args.profile} has no type')
        _ = FACTORY.load_plugin(adapter_type)
        config = RuntimeConfig.from_args(args)

        FACTORY.register_adapter(config)
        adapter = FACTORY.lookup_adapter(config.credentials.type)
        return adapter

    @staticmethod
    def _get_from_dict(dct: Dict[str, Any], keypath: Iterable[str]):
        value = dct
        for key in keypath:
            value = value[key]
        return value

    def _update_nested_dict(
        dct: Dict[str, Any], keypath: Iterable[str], value: Any
    ):
        next_key, keypath = keypath[0], keypath[1:]
        for cur_key in keypath:
            if next_key not in dct:
                dct[next_key] = {}
            dct = dct[next_key]
            next_key = cur_key
        dct[next_key] = value

    def get_fact(self, key):
        if isinstance(key, str) and key.startswith('fact.'):
            parts = key.split('.')[1:]
            try:
                return self._get_from_dict(self.project.facts, parts)
            except KeyError:
                pass
        return key

    def _relation_from_name(self, name: str):
        """reverse-engineer a relation (including quoting) from a given name and
        the adapter.

        This does assume that relations are split by the `.` character.

        Note that this doesn't really have to be correct, it only has to
        round-trip properly. Still, do our best to get this right.
        """
        cls = self.adapter.Relation
        credentials = self.adapter.config.credentials
        quote_policy = cls.get_default_quote_policy().to_dict()
        include_policy = cls.get_default_include_policy().to_dict()
        kwargs = {}

        parts = name.split('.')
        if len(parts) == 0:  # I think this is literally impossible!
            raise TestProcessingException(f'Invalid test name {name}')

        names = ['database', 'schema', 'identifier']
        defaults = [credentials.database, credentials.schema, None]
        values = chain(repeat(None, 3 - len(parts)), parts)
        for name, value, default in zip(names, values, defaults):
            # no quote policy -> use the default
            if value is None:
                if default is None:
                    include_policy[name] = False
                value = default
            else:
                include_policy[name] = True
                # if we have a value, we can figure out the quote policy.
                trimmed = value[1:-1]
                if self.adapter.quote(trimmed) == value:
                    quote_policy[name] = True
                    value = trimmed
                else:
                    quote_policy[name] = False
            kwargs[name] = value

        return cls.create(
            include_policy=include_policy,
            quote_policy=quote_policy,
            **kwargs
        )

    def step_dbt(self, sequence_item, tmpdir):
        if 'cmd' not in sequence_item:
            raise TestProcessingException(
                f'Got item type cmd, but no cmd in {sequence_item}'
            )
        cmd = shlex.split(sequence_item['cmd'])
        partial_parse = sequence_item.get('partial_parse', False)
        extra = [
            '--target', 'default',
            '--profile', 'dbt-pytest',
            '--profiles-dir', tmpdir,
            '--project-dir', os.path.join(tmpdir, 'project')
        ]
        base_cmd = ['dbt', '--debug']

        if partial_parse:
            base_cmd.append('--partial-parse')
        else:
            base_cmd.append('--no-partial-parse')

        full_cmd = base_cmd + cmd + extra
        cli_vars = sequence_item.get('vars', {}).copy()
        cli_vars.update(self._base_vars())
        if cli_vars:
            full_cmd.extend(('--vars', yaml.safe_dump(cli_vars)))
        expect_passes = sequence_item.get('check', True)
        result = run(full_cmd, check=False, capture_output=True)
        print(result.stdout.decode('utf-8'))
        if expect_passes:
            if result.returncode != 0:
                raise TestProcessingException(
                    f'Command {full_cmd} failed, expected pass! Got '
                    f'rc={result.returncode}'
                )
        else:
            if result.returncode == 0:
                raise TestProcessingException(
                    f'Command {full_cmd} passed, expected failure! Got '
                    f'rc={result.returncode}'
                )
        return result

    @staticmethod
    def _build_expected_attributes_dict(
        values: Dict[str, Any]
    ) -> Dict[str, Any]:
        # turn keys into nested dicts
        attributes = {}
        for key, value in values.items():
            parts = key.split('.', 1)
            if len(parts) != 2:
                raise TestProcessingException(
                    f'Expected a longer keypath, only got "{key}" '
                    '(no attributes?)'
                )
            name, keypath = parts

            if name not in attributes:
                attributes[name] = {}
            attributes[name][keypath] = value
        return attributes

    def step_run_results(self, sequence_item, tmpdir):
        path = os.path.join(tmpdir, 'project', 'target', 'run_results.json')

        expect_exists = sequence_item.get('exists', True)

        assert expect_exists == os.path.exists(path)
        if not expect_exists:
            return None

        try:
            with open(path) as fp:
                run_results_data = json.load(fp)
        except Exception as exc:
            raise DBTException(
                f'could not load run_results.json: {exc}'
            ) from exc
        try:
            results = run_results_data['results']
        except KeyError:
            raise DBTException(
                'Invalid run_results.json - no results'
            ) from None
        if 'length' in sequence_item:
            expected = self.get_fact(sequence_item['length'])
            assert expected == len(results)
        if 'names' in sequence_item:
            expected_names = set(self.get_fact(sequence_item['names']))
            extra_results_ok = sequence_item.get('extra_results_ok', False)

            for result in results:
                try:
                    name = result['node']['name']
                except KeyError as exc:
                    raise DBTException(
                        f'Invalid result, missing required key {exc}'
                    ) from None
                if (not extra_results_ok) and (name not in expected_names):
                    raise DBTException(
                        f'Got unexpected name {name} in results'
                    )
                expected_names.discard(name)
            if expected_names:
                raise DBTException(
                    f'Nodes missing from run_results: {list(expected_names)}'
                )
        if 'attributes' in sequence_item:
            values = self.get_fact(sequence_item['attributes'])

            attributes = self._build_expected_attributes_dict(values)

            for result in results:
                try:
                    node = result['node']
                    name = node['name']
                except KeyError as exc:
                    raise DBTException(
                        f'Invalid result, missing required key {exc}'
                    ) from None

                if name in attributes:
                    for key, value in attributes[name].items():
                        try:
                            self._get_from_dict(result, key.split('.'))
                        except KeyError as exc:
                            raise DBTException(
                                f'Invalid result, missing required key {exc}'
                            ) from None

    def _expected_catalog_member(self, sequence_item, catalog, member_name):
        if member_name not in catalog:
            raise DBTException(
                f'invalid catalog.json: no {member_name}!'
            )

        actual = catalog[member_name]
        expected = sequence_item.get(member_name, {})
        if 'length' in expected:
            expected_length = self.get_fact(expected['length'])
            assert len(actual) == expected_length

        if 'names' in expected:
            extra_nodes_ok = expected.get('extra_nodes_ok', False)
            expected_names = set(self.get_fact(expected['names']))
            for node in actual.values():
                try:
                    name = node['metadata']['name']
                except KeyError as exc:
                    singular = member_name[:-1]
                    raise TestProcessingException(
                        f'Invalid catalog {singular}: missing key {exc}'
                    ) from None
                if (not extra_nodes_ok) and (name not in expected_names):
                    raise DBTException(
                        f'Got unexpected name {name} in catalog'
                    )
                expected_names.discard(name)
            if expected_names:
                raise DBTException(
                    f'{member_name.title()} missing from run_results: '
                    f'{list(expected_names)}'
                )

    def step_catalog(self, sequence_item, tmpdir):
        path = os.path.join(tmpdir, 'project', 'target', 'catalog.json')
        expect_exists = sequence_item.get('exists', True)

        assert expect_exists == os.path.exists(path)
        if not expect_exists:
            return None

        try:
            with open(path) as fp:
                catalog = json.load(fp)
        except Exception as exc:
            raise DBTException(
                f'could not load catalog.json: {exc}'
            ) from exc

        self._expected_catalog_member(sequence_item, catalog, 'nodes')
        self._expected_catalog_member(sequence_item, catalog, 'sources')

    def step_relations_equal(self, sequence_item):
        if 'relations' not in sequence_item:
            raise TestProcessingException(
                'Invalid relations_equal: no relations'
            )
        relation_names = self.get_fact(sequence_item['relations'])
        assert isinstance(relation_names, list)
        if len(relation_names) < 2:
            raise TestProcessingException(
                'Not enough relations to compare',
            )
        relations = [
            self._relation_from_name(name) for name in relation_names
        ]
        with self.adapter.connection_named('_test'):
            basis, compares = relations[0], relations[1:]
            columns = [
                c.name for c in self.adapter.get_columns_in_relation(basis)
            ]

            for relation in compares:
                sql = self.adapter.get_rows_different_sql(
                    basis, relation, column_names=columns
                )
                _, tbl = self.adapter.execute(sql, fetch=True)
                num_rows = len(tbl)
                assert num_rows == 1, f'Invalid sql query from get_rows_different_sql: incorrect number of rows ({num_rows})'
                num_cols = len(tbl[0])
                assert num_cols == 2, f'Invalid sql query from get_rows_different_sql: incorrect number of cols ({num_cols})'
                row_count_difference = tbl[0][0]
                assert row_count_difference == 0, f'Got {row_count_difference} difference in row count betwen {basis} and {relation}'
                rows_mismatched = tbl[0][1]
                assert rows_mismatched == 0, f'Got {rows_mismatched} different rows between {basis} and {relation}'

    def step_relation_rows(self, sequence_item):
        if 'name' not in sequence_item:
            raise TestProcessingException('Invalid relation_rows: no name')
        if 'length' not in sequence_item:
            raise TestProcessingException('Invalid relation_rows: no length')
        name = self.get_fact(sequence_item['name'])
        length = self.get_fact(sequence_item['length'])
        relation = self._relation_from_name(name)
        with self.adapter.connection_named('_test'):
            _, tbl = self.adapter.execute(
                f'select count(*) as num_rows from {relation}',
                fetch=True
            )

        assert len(tbl) == 1 and len(tbl[0]) == 1, \
            'count did not return 1 row with 1 column'
        assert tbl[0][0] == length, \
            f'expected {name} to have {length} rows, but it has {tbl[0][0]}'

    def _generate_update_clause(self, clause) -> str:
        if 'type' not in clause:
            raise TestProcessingException(
                'invalid update_rows clause: no type'
            )
        clause_type = clause['type']

        if clause_type == 'add_timestamp':
            if 'src_col' not in clause:
                raise TestProcessingException(
                    'Invalid update_rows clause: no src_col'
                )
            add_to = self.get_fact(clause['src_col'])
            kwargs = {
                k: self.get_fact(v) for k, v in clause.items()
                if k in ('interval', 'number')
            }
            with self.adapter.connection_named('_test'):
                return self.adapter.timestamp_add_sql(
                    add_to=add_to,
                    **kwargs
                )
        elif clause_type == 'add_string':
            if 'src_col' not in clause:
                raise TestProcessingException(
                    'Invalid update_rows clause: no src_col'
                )
            if 'value' not in clause:
                raise TestProcessingException(
                    'Invalid update_rows clause: no value'
                )
            src_col = self.get_fact(clause['src_col'])
            value = self.get_fact(clause['value'])
            location = clause.get('location', 'append')
            with self.adapter.connection_named('_test'):
                return self.adapter.string_add_sql(
                    src_col, value, location
                )
        else:
            raise TestProcessingException(
                f'Unknown clause type in update_rows: {clause_type}'
            )

    def step_relation_types(self, sequence_item):
        """
        type: relation_types
        expect:
            foo: view
            bar: table
        """
        if 'expect' not in sequence_item:
            raise TestProcessingException('Invalid relation_types: no expect')
        expected = self.get_fact(sequence_item['expect'])

        expected_relation_values = {}
        found_relations = []
        schemas = set()

        for key, value in expected.items():
            relation = self._relation_from_name(key)
            expected_relation_values[relation] = value
            schemas.add(relation.without_identifier())
        with self.adapter.connection_named('__test'):
            for schema in schemas:
                found_relations.extend(self.adapter.list_relations_without_caching(schema))

        for key, value in expected.items():
            for relation in found_relations:
                # this might be too broad
                if relation.identifier == key:
                    assert relation.type == value, (
                        f'Got an unexpected relation type of {relation.type} '
                        f'for relation {key}, expected {value}'
                    )

    def step_update_rows(self, sequence_item):
        """
            type: update_rows
            name: base
            dst_col: some_date
            clause:
              type: add_timestamp
              src_col: some_date
            where: id > 10
        """
        if 'name' not in sequence_item:
            raise TestProcessingException('Invalid update_rows: no name')
        if 'dst_col' not in sequence_item:
            raise TestProcessingException('Invalid update_rows: no dst_col')

        if 'clause' not in sequence_item:
            raise TestProcessingException('Invalid update_rows: no clause')

        clause = self.get_fact(sequence_item['clause'])
        if isinstance(clause, dict):
            clause = self._generate_update_clause(clause)

        where = None
        if 'where' in sequence_item:
            where = self.get_fact(sequence_item['where'])

        name = self.get_fact(sequence_item['name'])
        dst_col = self.get_fact(sequence_item['dst_col'])
        relation = self._relation_from_name(name)

        with self.adapter.connection_named('_test'):
            sql = self.adapter.update_column_sql(
                dst_name=str(relation),
                dst_column=dst_col,
                clause=clause,
                where_clause=where,
            )
            self.adapter.execute(sql, auto_begin=True)
            self.adapter.commit_if_has_connection()

    def _write_profile(self, tmpdir):
        profile_data = {
            'config': {
                'send_anonymous_usage_stats': False,
            },
            'dbt-pytest': {
                'target': 'default',
                'outputs': {
                    'default': self.target,
                },
            },
        }
        with open(os.path.join(tmpdir, 'profiles.yml'), 'w') as fp:
            fp.write(yaml.safe_dump(profile_data))

    def _add_context(self, error_str, idx, test_item):
        item_type = test_item['type']
        return f'{error_str} in test index {idx} (item_type={item_type})'

    def run_test_item(self, idx, test_item, tmpdir):
        try:
            item_type = test_item['type']
        except KeyError:
            raise TestProcessingException(
                f'Could not find type in {test_item}'
            ) from None
        print(f'Executing step {idx+1}/{len(self.sequence)}')
        try:
            if item_type == 'dbt':
                assert os.path.exists(tmpdir)
                self.step_dbt(test_item, tmpdir)
            elif item_type == 'run_results':
                self.step_run_results(test_item, tmpdir)
            elif item_type == 'catalog':
                self.step_catalog(test_item, tmpdir)
            elif item_type == 'relations_equal':
                self.step_relations_equal(test_item)
            elif item_type == 'relation_rows':
                self.step_relation_rows(test_item)
            elif item_type == 'update_rows':
                self.step_update_rows(test_item)
            elif item_type == 'relation_types':
                self.step_relation_types(test_item)
            else:
                raise TestProcessingException(
                    f'Unknown item type {item_type}'
                )
        except AssertionError as exc:
            if len(exc.args) == 1:
                arg = self._add_context(exc.args[0], idx, test_item)
                exc.args = (arg,)
            else:  # uhhhhhhh
                exc.args = exc.args + (self._add_context('', idx, test_item),)
            raise

    def runtest(self):
        FACTORY.reset_adapters()
        with tempfile.TemporaryDirectory() as tmpdir:
            self._write_profile(tmpdir)
            self.project.write(tmpdir)
            self.adapter = self._get_adapter(tmpdir)

            self.schema_relation = self.adapter.Relation.create(
                database=self.adapter.config.credentials.database,
                schema=self.adapter.config.credentials.schema,
                quote_policy=self.adapter.config.quoting,
            )

            try:
                for idx, test_item in enumerate(self.sequence):
                    self.run_test_item(idx, test_item, tmpdir)
            finally:
                with self.adapter.connection_named('__test'):
                    if self.config.getoption('drop_schema'):
                        self.adapter.drop_schema(self.schema_relation)

        return True

    def repr_failure(self, excinfo):
        """ called when self.runtest() raises an exception. """
        if isinstance(excinfo.value, DBTException):
            return "\n".join([
                "usecase execution failed",
                "   spec failed: {!r}".format(excinfo.value.args),
                "   no further details known at this point.",
            ])
        elif isinstance(excinfo.value, CalledProcessError):
            failed = str(excinfo.value.cmd)
            stdout = excinfo.value.stdout.decode('utf-8')
            stderr = excinfo.value.stderr.decode('utf-8')
            return '\n'.join([
                f'failed to execute "{failed}:',
                f'   output: {stdout}',
                f'   error: {stderr}',
                f'   rc: {excinfo.value.returncode}',
            ])
        elif isinstance(excinfo.value, TestProcessingException):
            return str(excinfo.value)
        else:
            return f'Unknown error: {excinfo.value}'

    def reportinfo(self):
        return self.fspath, 0, "usecase: {}".format(self.name)
