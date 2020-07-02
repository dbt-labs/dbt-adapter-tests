A test suite for adapter plugins.

This test suite requires that your adapter implement a `get_rows_different_sql` with this signature:

    def get_rows_different_sql(
            self,
            relation_a: BaseRelation,
            relation_b: BaseRelation,
            column_names: Optional[List[str]] = None,
        ) -> str:

This is available in the `feature/adapter-test-suite-changes` branch of dbt-core.

You'll need to install this package and write a specfile, which is just a yaml file ending in `.dbtspec`. See the included spark/postgres examples in `specs`. You can also author custom test sequences and override existing default projects (see spark for an example of the latter).

Then, to run it, a command like `pytest -p pytest_dbt_adapter path/to/mytest.dbtspec`. You'll need dbt-core and your adapter plugin installed in the environment.
