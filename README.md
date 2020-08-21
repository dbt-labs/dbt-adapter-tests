A test suite for adapter plugins.

## Installation and use


`pip install pytest-dbt-adapter`


You'll need to install this package with `pip install pytest-dbt-adapter` and write a specfile, which is a yaml file ending in `.dbtspec`. See the included spark/postgres examples in `specs`. You can also write custom test sequences and override existing default projects.

After installing this package, you should be able to run your spec with `pytest path/to/mytest.dbspec`. You'll need dbt-core and your adapter plugin installed in the environment as well.


## Specs

A spec is composed of a minimum of two things:
  - a `target` block
  - a `sequences` block
    - The keys are test names. You can select from these names with pytest's `-k` flag.
    - The values are test sequence definitions.

Optionally, there is also:
  - a `projects` block

### Targets

A target block is just like a target block you'd use in dbt core. However, there is one special change: the `schema` field should include a `{{ var('_dbt_random_suffix') }}` somewhere that the test suite will insert.


### Sequences

A sequence has a `name` (the sequence name), a `project` (the project name to use), and `sequence` (a collection of test steps). You can declare new sequences inline, or use the name of a builtin sequence. A sequence itself is just a list of steps. You can find examples in the form of the builtin sequences in the `sequences/` folder.

You are encouraged to use as many sequences as you can from the built-in list without modification.


### Projects

The minimum project contains only a `name` field. The value is the name of the project - sequences include a project name.

A project also has an optional `paths` block, where the keys are relative file paths (to a `dbt_project.yml` that will be written), and the values are the contents of those files.

There is a `dbt_project_yml` block, which should be a dictionary that will be updated into the default dbt_project.yml (which sets name, version, and config-version).


Instead of declaring a `name` field, a project definition may have an `overrides` field that names a builtin project. The test suite will update the named builtin project with those overrides, instead of overwriting the full project with a new one.
