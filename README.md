# air2neo
[![Test Lint and Deploy](https://github.com/bilbyai/air2neo/actions/workflows/test_lint_deploy.yml/badge.svg)](https://github.com/bilbyai/air2neo/actions/workflows/test_lint_deploy.yml)
[![codecov](https://codecov.io/gh/bilbyai/air2neo/branch/main/graph/badge.svg?token=EQW6XHZSXS)](https://codecov.io/gh/bilbyai/air2neo)

Airtable to Neo4j data ingestor


## Quickstart
```python
from air2neo import Air2Neo, MetatableConfig

a2n = Air2Neo(
    airtable_api_key,
    airtable_base_id,
    neo4j_uri,
    neo4j_username,
    neo4j_password,
    MetatableConfig(
        table_name,                         # "Metatable"
        # Optionally, you can provide `table`,
        # which is a pyairtable.Table object.

        name_col,                           # "Name",
        view_col,                           # "View", (optional)
        index_for_col,                      # "IndexFor",
        constrain_for_col,                  # "ConstrainFor",
        node_properties_col,                # "NodeProperties",
        edges_col,                          # "Edges",
        node_properties_last_ingested_col,  # "nodesLastIngested",
        edges_last_ingested_col,            # "edgesLastIngested",
        airtable_id_property_in_neo4j,      # "_aid" (The name of the property in Neo4j that stores the Airtable ID, defaults to)
        format_edge_col_name,               # "function that formats edge column names. Removes everything after a double-underscore, e.g. IN_INDUSTRY__BANK is renamed to IN_INDUSTRY",
        airtable_api_key,                   # "Airtable API key",
        airtable_base_id,                   # "Airtable base ID",
    ),
)
a2n.run()
```

If you have a .env file like so:
```
AIRTABLE_API_KEY=
AIRTABLE_BASE_ID=
AIRTABLE_METATABLE_NAME=        # Optional, defaults to "Metatable"
NEO4J_URI=
NEO4J_USERNAME=
NEO4J_PASSWORD=
```
You just run the following:
```python
from air2neo import Air2Neo

a2n = Air2Neo()
a2n.run()
```
## Installation
```bash
pip install air2neo
```

Or run it without installing:

```bash
uvx air2neo
```

## Run the migration
Set the required environment variables, either in your shell or in a `.env` file:

```bash
AIRTABLE_API_KEY=
AIRTABLE_BASE_ID=
AIRTABLE_METATABLE_NAME=Metatable
NEO4J_URI=
NEO4J_USERNAME=
NEO4J_PASSWORD=
```

Run the migration directly with uvx:

```bash
uvx air2neo
```

CLI arguments override environment variables and `.env` values:

```bash
uvx air2neo \
  --airtable-api-key "$AIRTABLE_API_KEY" \
  --airtable-base-id "$AIRTABLE_BASE_ID" \
  --metatable-name "Metatable" \
  --neo4j-uri "$NEO4J_URI" \
  --neo4j-username "$NEO4J_USERNAME" \
  --neo4j-password "$NEO4J_PASSWORD"
```

Config priority is:

1. CLI arguments
2. Environment variables, including values loaded from `.env`
3. Built-in default for `AIRTABLE_METATABLE_NAME`, which is `Metatable`

To use an explicit command name:

```bash
uvx --from air2neo air2neo-migrate
```

For local development:

```bash
uv sync --group dev
uv run air2neo
```

## Release
```bash
uv version --bump patch
git add pyproject.toml uv.lock
git commit -m "Bump version to $(uv version --short)"
git push origin main
```

Pushing a bumped version to `main` publishes the package to PyPI after CI passes.
The publish job checks PyPI first, so pushes that do not change the version are skipped.

You can also publish locally:

```bash
uv build --no-sources --sdist --wheel --out-dir dist/
uv publish dist/*
```
## Documentation
To be implemented. For now, please look at the code docstrings. Sorry about that!
