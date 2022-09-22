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
        index_for_col,                      # "IndexFor",
        constrain_for_col,                  # "ConstrainFor",
        node_properties_col,                # "NodeProperties",
        edges_col,                          # "Edges",
        node_properties_last_ingested_col,  # "nodesLastIngested",
        edges_last_ingested_col,            # "edgesLastIngested",
        airtable_id_property_in_neo4j,      # "_aid" (The name of the property in Neo4j that stores the Airtable ID, defaults to)
        format_edge_col_name,               # "function that formats edge column names. Removes everything after a double-underscore, e.g. IN_INDUSTRY__BANK is renamed to IN_INDUSTRY",
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
$ pip install air2neo
```
## Documentation
To be implemented. For now, please look at the code docstrings. Sorry about that!
