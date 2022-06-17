import logging
from logging.config import dictConfig
from os import environ
from time import perf_counter
from typing import Any, Callable, Dict, Sequence, Tuple

from neo4j import GraphDatabase, Transaction
from pandas import DataFrame, Series
from pyairtable import Table

from .config import (format_edge_col_name_default, is_edge_rule_default,
                     is_prop_rule_default, keep_col_rule_default)


class Air2Neo:
    def __init__(
        self,
        /,
        airtable_api_key: str = environ.get("AIRTABLE_API_KEY", None),
        airtable_base_id: str = environ.get("AIRTABLE_BASE_ID", None),
        airtable_table_name: str = environ.get("AIRTABLE_METATABLE_NAME", "Metatable"),
        neo4j_uri: str = environ.get("NEO4J_URI", None),
        neo4j_username: str = environ.get("NEO4J_USERNAME", None),
        neo4j_password: str = environ.get("NEO4J_PASSWORD", None),
        *,  # Only allow keyword arguments after this point
        neo4j_driver: GraphDatabase = None,  # Optional if above is provided
        airtable_metatable: Table = None,  # Optional if above is provided
        neo4j_airtable_id_property: str = "_aid",
        keep_col_rule: Callable = keep_col_rule_default,
        is_prop_rule: Callable = is_prop_rule_default,
        is_edge_rule: Callable = is_edge_rule_default,
        format_edge_col_name: Callable = format_edge_col_name_default,
        edge_label: str = "label",
        edge_source: str = "source",
        edge_target: str = "target",
        logger: logging.Logger = None,
    ):
        """The constructor for the Air2Neo class.

        There are a few ways to instantiate this Air2Neo class.

        Method 1: If you have the following environment variables set:
        AIRTABLE_API_KEY, AIRTABLE_BASE_ID, NEO4J_URI, NEO4J_USERNAME,
        NEO4J_PASSWORD, then you can instantiate the class like this:
        ```
        from air2neo import Air2Neo
        air2neo = Air2Neo()
        air2neo.run()
        ```

        Method 2: You can supply the value of the environment variables manually.
        ```
        from air2neo import Air2Neo
        air2neo = Air2Neo(
            airtable_api_key=<your_airtable_api_key>,
            airtable_base_id=<your_airtable_base_id>,
            neo4j_uri=<your_neo4j_uri>,
            neo4j_username=<your_neo4j_username>,
            neo4j_password=<your_neo4j_password>,
        )
        air2neo.run()
        ```

        Method 3: You can pass in your own Airtable "Metatable" and Neo4j driver.
        ```
        from air2neo import Air2Neo
        from neo4j import GraphDatabase
        from pyairtable import Table
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

        metatable = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, 'Metatable')
        # or, you can provide the airtable table ID:
        # metatable = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, <metatable_id>)

        air2neo = Air2Neo(
            airtable_metatable=metatable,
            neo4j_driver=driver,
        )
        air2neo.run()
        ```

        Args:
            airtable_api_key (str, optional): The Airtable API key. Defaults to None.
            airtable_base_id (str, optional): The Airtable base ID. Defaults to None.
            neo4j_username (str, optional): The Neo4j username. Defaults to None.
            neo4j_password (str, optional): The Neo4j password. Defaults to None.
            neo4j_uri (str, optional): The Neo4j URI. Defaults to None.
            airtable_metatable (pyairtable.Table, optional): Optional if airtable_api_key and
                airtable_base_id are provided, and will look for 'Metatable' within the base if not
                specified. Defaults to None.
            neo4j_airtable_id_property (str, optional): The name of the property that will be used
                to store the Airtable ID. Defaults to '_aid'.
            keep_col_rule (Callable, optional): _description_. Defaults to keep_col_rule.
            is_prop_rule (Callable, optional): _description_. Defaults to is_prop_rule.
            is_edge_rule (Callable, optional): A function, given the string column name, returns a
                boolean whether the column is an edge. Defaults to is_edge_rule_default.
                The default function check if the column name is all caps.
            format_edge_col_name (Callable, optional): _description_. Defaults to format_edge_col_name.
            logger (logging.Logger, optional): _description_. Defaults to None.
            edge_label (str, optional): _description_. Defaults to 'label'.
            edge_source (str, optional): _description_. Defaults to 'source'.
            edge_target (str, optional): _description_. Defaults to 'target'.

        Raises:
            ValueError: If neo4j_driver is not provided, and neo4j_uri,
            neo4j_username, and neo4j_password are not provided.
        """

        # Create logger, if not already configured
        self.logger = logger if logger else Air2Neo._create_logger(log_level="INFO")

        # Validate Neo4j driver
        # Configure neo4j driver, if not already configured
        if not neo4j_driver and not (neo4j_uri and neo4j_username and neo4j_password):
            # If no driver is provided, and no neo4j_uri, neo4j_username, and neo4j_password
            # are provided, then raise an error
            raise ValueError(
                "If no neo4j_driver is provided, then neo4j_uri, "
                "neo4j_username, and neo4j_password must be provided."
            )

        if neo4j_driver:
            self.neo4j_driver = neo4j_driver

        else:
            self.logger.info("Creating Neo4j driver...")
            self.neo4j_driver = GraphDatabase.driver(
                neo4j_uri, auth=(neo4j_username, neo4j_password)
            )

        self.airtable_api_key = airtable_api_key
        self.airtable_base_id = airtable_base_id
        self.airtable_table_name = airtable_table_name

        # Validate Airtable
        if not airtable_metatable:
            # If no airtable_metatable is provided, then infer from keys
            if not (airtable_api_key and airtable_base_id):
                # If no airtable_api_key or airtable_base_id are provided, then raise an error
                raise ValueError(
                    "If no airtable_metatable is provided, then "
                    "airtable_api_key and airtable_base_id must be provided."
                )

            self.airtable_metatable = Table(
                airtable_api_key,
                airtable_base_id,
                airtable_table_name if airtable_table_name else "Metatable",
            )

        else:
            # pyairtable.Table is provded, will infer api key and base id from it
            self.airtable_metatable = airtable_metatable
            if not airtable_api_key:
                self.airtable_api_key = self.airtable_metatable.api_key
            if not airtable_base_id:
                self.airtable_base_id = self.airtable_metatable.base_id

        # Default Values
        self.id_property = neo4j_airtable_id_property
        self.is_edge_rule = is_edge_rule
        self.is_prop_rule = is_prop_rule
        self.keep_col_rule = keep_col_rule
        self.format_edge_col_name = format_edge_col_name
        self.edge_label = edge_label
        self.edge_source = edge_source
        self.edge_target = edge_target

    def run(self, clean_ingest: bool = False) -> None:
        """_summary_

        Args:
            clean_ingest (bool, optional): If set to true, the Neo4j database
            will be completely dropped before ingesting data. Defaults to False.

        Returns:
            None
        """
        self.logger.info("Starting Airtable to Neo4j ingest job.")
        start_time = perf_counter()

        tables = [x["fields"]["Name"] for x in self.airtable_metatable.all()]
        self.logger.info("Found %s tables in Airtable: %s", len(tables), tables)

        airtables = [
            Table(self.airtable_api_key, self.airtable_base_id, t) for t in tables
        ]

        # This part is in a for-loop because I wanted to convert this to a
        # concurrent job.
        dfs = []
        for table in airtables:
            dfs.append(self._download_airtable_and_return_as_df(table))

        del tables, airtables

        self.logger.info("Creating Neo4j session...")
        with self.neo4j_driver.session() as session:

            # If clean_ingest is set to true, drop the entire database.
            if clean_ingest:
                self.logger.info("Dropping entire Neo4j database...")
                session.run("MATCH (n) DETACH DELETE n")

            # Create Nodes
            for table, df in dfs:
                self.logger.info('Creating nodes for table "%s"...', table)

                def _make_node_list(row):
                    node = row["props"]
                    node[self.id_property] = row["id"]
                    return node

                node_list = df.apply(_make_node_list, axis=1).to_list()

                with session.begin_transaction() as tx:
                    self.logger.info(
                        'Creating %s nodes for table "%s"...', len(node_list), table
                    )
                    self.batch_create_node(tx, label=table, node_list=node_list)
                    self.logger.info(
                        '%s nodes created/merged for table "%s".', len(node_list), table
                    )
                    tx.commit()

                # Create Constraint
                self.logger.info('Creating constraint for table "%s"...', table)
                with session.begin_transaction() as tx:
                    self.create_constraint_for(
                        tx, label=table, constraint=self.id_property
                    )
                    tx.commit()

            # Create Edges
            for table, df in dfs:
                self.logger.info('Creating edge dict for table "%s"...', table)
                edge_list = []
                for _, row in df.iterrows():
                    row_id, edges = row["id"], row["edges"]
                    for k, v in edges.items():
                        for v_ in v:
                            edge_list.append(
                                {
                                    self.edge_source: row_id,
                                    self.edge_target: v_,
                                    self.edge_label: k,
                                }
                            )

                self.logger.info(
                    'Creating %s edges for table "%s"...', len(edge_list), table
                )
                with session.begin_transaction() as tx:
                    self.batch_create_edge(tx, edge_list=edge_list)
                    tx.commit()

                self.logger.info(
                    '%s edges created/merged for table "%s".', len(edge_list), table
                )

        # Close driver
        self.neo4j_driver.close()

        # Print elapsed time
        self.logger.info(
            "Airtable to Neo4j ingest job completed in %0.2f seconds.",
            perf_counter() - start_time,
        )

    def _download_airtable_and_return_as_df(
        self, table: Table
    ) -> Tuple[str, DataFrame]:
        """Downloads a single Airtable table and returns it as a DataFrame.

        Args:
            table (Table): A single Airtable table.

        Returns:
            Tuple[str, DataFrame]: A tuple containing the name of the table,
            and the DataFrame containing the table's data.
        """
        name = table.table_name
        self.logger.info("Downloading Airtable table %s", name)
        start_time = perf_counter()
        df = DataFrame(table.all())
        self.logger.info(
            "Downloaded Airtable table %s (Records: %s) in %0.2f seconds",
            name,
            len(df),
            perf_counter() - start_time,
        )
        df = df.apply(lambda row: Air2Neo._split_node_edge(row), axis=1)
        return name, df

    def create_index_for(self, tx: Transaction, label: str, indexes: Sequence[str]):
        """Creates an index for a label.

        Args:
            tx (Transaction): The Neo4j transaction to use.
            label (str): The label to create an index for.
            indexes (Sequence[str]): The indexes to create.
            log (Any, optional): The logger to use. Defaults to logger.

        Returns:
            _type_: _description_
        """
        index_query = ", ".join([f"n.{index}" for index in indexes])
        cypher = f"""
CREATE INDEX IF NOT EXISTS FOR (n.{label})
ON ({index_query})"""
        res = tx.run(cypher)
        return res

    def create_constraint_for(self, tx: Transaction, label: str, constraint: str):
        """Creates a constraint for a label.

        Args:
            tx (Transaction): The Neo4j transaction to use.
            label (str): The label to create a constraint for.
            constraint (str): The constraint to create.
            log (Any, optional): The logger to use. Defaults to logger.
        """
        cypher = f"""
CREATE CONSTRAINT IF NOT EXISTS ON (n:{label})
ASSERT n.{constraint} IS UNIQUE"""
        res = tx.run(cypher)
        return res

    def batch_create_node(
        self, tx: Transaction, label: str, node_list: Sequence[Dict[str, Any]]
    ):
        """Creates a batch of nodes.

        Args:
            tx (Transaction): The Neo4j transaction to use.
            label (str): The label of the nodes.
            node_list (Sequence[Dict[str, Any]]): The list of nodes to create.
            log (Any, optional): The logger to use. Defaults to logger.
        """
        cypher = f"""
UNWIND $node_list AS node
MERGE (n:{label} {{{self.id_property}: node.{self.id_property}}})
SET n = node"""
        res = tx.run(cypher, node_list=node_list)
        return res

    def batch_create_edge(self, tx: Transaction, edge_list: Sequence[Dict[str, str]]):
        """Creates a batch of edges.

        Args:
            tx (Transaction): The Neo4j transaction to use.
            edge_list (Sequence[Dict[str, str]]): The list of edges to create.
            The dict format is:
            {
                'source': '<source_id>',
                'target': '<target_id>',
                'label': '<edge_label>',
            }
            The name of the dict keys are in config, and are the following, respectively:
                edge_source, edge_target, edge_label.
            log (Any, optional): The logger to use. Defaults to logger.
        """
        cypher = f"""
UNWIND $edge_list AS edge
MATCH (n) WHERE n.{self.id_property} = edge.{self.edge_source}
MATCH (m) WHERE m.{self.id_property} = edge.{self.edge_target}
OPTIONAL MATCH (n)-[rel]-(m)
WITH n, m, edge, COLLECT(TYPE(rel)) AS relTypes
WHERE NOT edge.{self.edge_label} IN relTypes
CALL apoc.create.relationship(n, edge.{self.edge_label}, NULL, m)
YIELD rel
RETURN n, m, rel"""
        res = tx.run(cypher, edge_list=edge_list)
        return res

    def _split_node_edge(self, row: Series) -> Series:
        # This function is created just to do a df.apply()
        row["fields"] = {
            k: v for k, v in row["fields"].items() if self.keep_col_rule(k)
        }

        row["edges"] = {
            format_edge_col_name_default(k): v
            for k, v in row["fields"].items()
            if self.is_edge_rule(k)
        }

        row["props"] = {k: v for k, v in row["fields"].items() if self.is_prop_rule(k)}

        del row["fields"]

        if row["createdTime"]:
            del row["createdTime"]

        return row

    @staticmethod
    def _create_logger(log_level: str) -> logging.Logger:
        _log_config = dict(
            version=1,
            disable_existing_loggers=False,
            formatters={
                "default": {
                    "fmt": "%(levelprefix)s %(asctime)s %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
            handlers={
                "default": {
                    "formatter": "default",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                },
            },
            loggers={
                "air2neo": {"handlers": ["default"], "level": "INFO"},
            },
        )
        dictConfig(_log_config)
        return logging.getLogger("air2neo")
