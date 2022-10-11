import datetime
import logging
from enum import Enum
from logging.config import dictConfig
from os import environ
from time import perf_counter
from typing import Any, Callable, Dict, List, Literal, Sequence, Tuple

from neo4j import GraphDatabase
from pandas import DataFrame
from pyairtable import Table
from requests import HTTPError

from .config import format_edge_col_name_default
from .neo4j_operations import (
    neo4jop_batch_create_edge,
    neo4jop_batch_create_nodes,
    neo4jop_create_constraint_for_label,
    neo4jop_create_index_for_label,
)
from .utils import get_airtable_timestamp_str, is_airtable_record_id


def _create_logger(log_level: str = "INFO") -> logging.Logger:
    """Registers and returns a FastAPI-style logger object.

    Args:
        log_level (str, optional): The log level. Defaults to "INFO".

    Returns:
        logging.Logger: A logger object.
    """
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
            "air2neo": {"handlers": ["default"], "level": log_level},
        },
    )
    dictConfig(_log_config)
    return logging.getLogger("air2neo")


class IngestionUpdateType(Enum):
    """Enum for the type of data that is being updated in Neo4j.
    Types of data include:
        - Nodes
        - Edges
        - Node Properties
        - Edge Properties
    However, Nodes are simply labels in Neo4j, so they are not included here.
    Edge properties are also not included because Airtable does not support
    edge properties natively, so any implementation is a workaround. They will
    not be implemented at this time.
    """

    edges: Literal["Edges"] = "Edges"
    node_properties: Literal["NodeProperties"] = "NodeProperties"


class MetatableConfig:
    """The metatable is a table that contains instructions for how to ingest
    data from Airtable into Neo4j. It contains the following columns:
        - Name: The name of the label in Neo4j
        - IndexFor: A multiple-select column that contains the names of the
            columns in the Airtable table that should be indexed for the label.
            This makes the columns searchable in Neo4j.
        - ConstrainFor: A multiple-select column that contains the names of the
            columns in the Airtable table that should be constrained for the
            label. This makes each value in the columns unique in Neo4j.
        - NodeProperties: A multiple-select column that contains the names of the
            columns in the Airtable table that should be added as properties to
            the nodes in Neo4j.
        - Edges: A multiple-select column that contains the names of the columns
            in the Airtable table that should be added as edges to the nodes in
            Neo4j.
        - NodePropertiesLastIngested: A timestamp column that contains the last
            time the node properties were ingested into Neo4j. This will be
            updated after each ingestion.
        - EdgesLastIngested: A timestamp column that contains the last time the
            edges were ingested into Neo4j. This will be updated after each
            ingestion.
        - TranslationId: A single-line text column that contains the name of the
            column in the Airtable table that contains the ID if the table is a
            synced table and needs to be translated to a different ID.
    """

    # pylint: disable=too-many-instance-attributes
    # 16 is necessary for this class.

    def __init__(
        self,
        table: Table = None,
        name_col: str = "Name",
        index_for_col: str = "IndexFor",
        constrain_for_col: str = "ConstrainFor",
        node_properties_col: str = "NodeProperties",
        edges_col: str = "Edges",
        node_properties_last_ingested_col: str = "NodePropertiesLastIngested",
        edges_last_ingested_col: str = "EdgesLastIngested",
        translation_id_col: str = "TranslationId",
        airtable_id_property_in_neo4j: str = "_aid",
        format_edge_col_name: Callable[[str], str] = format_edge_col_name_default,
        airtable_api_key: str = environ.get("AIRTABLE_API_KEY", None),
        airtable_base_id: str = environ.get("AIRTABLE_BASE_ID", None),
        metatable_name: str = "Metatable",
    ):
        """Initialize the MetatableConfig object.

        Args:
            table (pyairtable.Table, optional):
                The pyairtable Table object for the Metatable. Defaults to None,
                but will be created if table_name is provided.
            name_col (str, optional):
                The name of the column in the Metatable that contains the table names in Airtable.
                e.g. If you have a table in Airtable called "Person", you should have a row in the
                Metatable with the value "Person" in the Name column. Defaults to "Name".
            index_for_col (str, optional):
                The name of the column in the Metatable that contains the names of the columns in
                the Airtable table that should be indexed for the label. This makes the columns
                searchable in Neo4j. Defaults to "IndexFor".
            constrain_for_col (str, optional):
                The name of the column in the Metatable that contains the names of the columns in
                the Airtable table that should be constrained for the label. This makes each value
                in the columns unique in Neo4j. Defaults to "ConstrainFor".
            node_properties_col (str, optional):
                The name of the column in the Metatable that contains the names of the columns in
                the Airtable table that should be added as properties to the nodes in Neo4j.
                Defaults to "NodeProperties".
            edges_col (str, optional):
                The name of the column in the Metatable that contains the names of the columns in
                the Airtable table that should be added as edges to the nodes in Neo4j.
                Defaults to "Edges".
            node_properties_last_ingested_col (str, optional):
                The name of the column in the Metatable that contains the last time the node
                properties were ingested into Neo4j. This will be updated after each ingestion.
                Defaults to "NodePropertiesLastIngested".
            edges_last_ingested_col (str, optional):
                The name of the column in the Metatable that contains the last time the edges were
                ingested into Neo4j. This will be updated after each ingestion.
                Defaults to "EdgesLastIngested".
            translation_id_col (str, optional):
                The name of the column in the Metatable that contains the name of the column in the
                Airtable table that contains the ID if the table is a synced table and needs to be
                translated to a different ID. Defaults to "TranslationId".
            airtable_id_property_in_neo4j (str, optional):
                The name of the property in Neo4j that will contain the Airtable ID for each node.
                Defaults to "_aid".
            format_edge_col_name (Callable[[str], str], optional):
                A function that takes the name of an edge column and returns the name of the edge
                property in Neo4j. Defaults to a function that removes everything after
                double-underscores, i.e. "LIVES_IN__CITY" becomes "LIVES_IN".
            airtable_api_key (str, optional):
                The Airtable API key. Defaults to the value of the AIRTABLE_API_KEY environment
                variable.
            airtable_base_id (str, optional):
                The Airtable base ID. Defaults to the value of the AIRTABLE_BASE_ID environment
                variable.
            metatable_name (str, optional):
                The string name of the Metatable. Defaults to "Metatable".
        """
        # pylint: disable=too-many-arguments

        self.metatable_name = metatable_name
        self.table = table

        # column mapping
        self.name_col = name_col
        self.index_for_col = index_for_col
        self.constrain_for_col = constrain_for_col
        self.node_properties_col = node_properties_col
        self.edges_col = edges_col
        self.node_properties_last_ingested_col = node_properties_last_ingested_col
        self.edges_last_ingested_col = edges_last_ingested_col
        self.translation_id_col = translation_id_col

        # configs
        self.airtable_id_property_in_neo4j = airtable_id_property_in_neo4j
        self.format_edge_col_name = format_edge_col_name

        # logging
        self.logger = _create_logger()

        self.table = None
        self.table_data = None
        self.ingestion_type_col_name_map = None
        self.label_airtableid_map = None
        self.column_instructions = None

        if table is not None:
            self.init_table(table)

        elif (
            airtable_api_key is not None
            and airtable_base_id is not None
            and self.metatable_name is not None
        ):
            self.init_table(
                Table(airtable_api_key, airtable_base_id, self.metatable_name)
            )
        else:
            self.logger.warning(
                "No table provided and no Airtable API key, base ID, or metatable name provided. "
                "You must call init_table(meta_table: pyairtable.Table) to initialize the "
                "metatable."
            )

    def init_table(self, meta_table: Table) -> None:
        """Embed the table reference, retrieve the table data, and do some preprocessing.

        Args:
            meta_table (Table): The pyairtable Table object for the Metatable.
        """
        self.table = meta_table
        self.table_data = self.table.all()

        self.ingestion_type_col_name_map = {
            IngestionUpdateType.node_properties: self.node_properties_last_ingested_col,
            IngestionUpdateType.edges: self.edges_last_ingested_col,
        }

        self.label_airtableid_map = self._create_label_airtableid_map(self.table_data)
        self.column_instructions = self._create_column_instructions(self.table_data)

    def validate(self) -> bool:
        """Validate all column names in the Metatable.

        Currently, this function doesn't check everything, but it will be expanded in the future.

        Returns:
            bool: True if all column names are valid, False otherwise.
        """
        self.label_airtableid_map = self._create_label_airtableid_map(self.table_data)
        self.column_instructions = self._create_column_instructions(self.table_data)
        result = True
        for label in self.column_instructions.keys():
            result = result and self.validate_column_names_for_label_in_airtable(label)
        return result

    def validate_column_names_for_label_in_airtable(
        self, label: str, max_records: int = 1000
    ) -> bool:
        """Validate that the column names in the Airtable table match the column names in the
        Metatable. This is useful to ensure that the Metatable is up to date with the Airtable
        table.

        Args:
            label (str): The label of the table in Airtable.
            max_records (int, optional): The maximum number of records to check. This is like how
                Excel will check the first 200 rows by default to check the data type when importing
                CSV files. Defaults to 1000.

        Returns:
            bool: True if all column names are found, False otherwise.
        """
        # _record = self.table.get(self.label_airtableid_map.get(label, None))
        self.logger.info("Validating column names in Airtable for label: %s", label)
        _record = [
            t for t in self.table_data if t["fields"].get(self.name_col, None) == label
        ][0]
        columns_to_look_for = []

        columns_to_look_for += _record["fields"].get(self.index_for_col, [])
        columns_to_look_for += _record["fields"].get(self.constrain_for_col, [])
        columns_to_look_for += _record["fields"].get(self.node_properties_col, [])
        columns_to_look_for += _record["fields"].get(self.edges_col, [])

        self.logger.debug("Columns to look for: %s", columns_to_look_for)

        label_table = Table(self.table.api_key, self.table.base_id, label)
        for records in label_table.iterate(page_size=100, max_records=max_records):
            for record in records:
                keys = record["fields"].keys()
                columns_to_look_for = [c for c in columns_to_look_for if c not in keys]
                if len(columns_to_look_for) == 0:
                    self.logger.info("Result is OK for label: %s", label)
                    return True

        self.logger.warning(
            "Could not find columns: %s in table %s after %s records",
            columns_to_look_for,
            label,
            max_records,
        )
        return False

    def _create_column_instructions(
        self, table_data: Sequence[Dict]
    ) -> Dict[str, Dict[str, str]]:
        """Create a map of column names to instructions for how to ingest that column.

        Args:
            table_data (Sequence[Dict]): The data from the Metatable

        Returns:
            Dict[str, Dict[str, str]]:
                A map of column names to instructions for how to ingest that column.
        """
        instructions = {
            t["fields"][self.name_col]: {
                "IndexFor": t["fields"].get(self.index_for_col, []),
                "ConstrainFor": t["fields"].get(self.constrain_for_col, []),
                "NodeProperties": t["fields"].get(self.node_properties_col, []),
                "Edges": t["fields"].get(self.edges_col, []),
                "TranslationId": t["fields"].get(self.translation_id_col, None),
            }
            for t in table_data
            if t["fields"].get(self.name_col, None)
        }

        return instructions

    def _create_label_airtableid_map(
        self, table_data: Sequence[Dict]
    ) -> Dict[str, str]:
        """Create a map of labels to airtable ids. What this means is that given a label name,
        like "Person", you can get the airtable id for that label in the Metatable, which is
        required for updating the last ingestion date for that label.

        Args:
            table_data (Sequence[Dict]): The data from the Metatable

        Returns:
            Dict[str, str]: A map of labels to airtable ids
        """
        return {
            t["fields"][self.name_col]: t["id"]
            for t in table_data
            if t["fields"].get(self.name_col, None)
        }

    def update_last_ingestion_date(
        self,
        label: str,
        ingestionType: IngestionUpdateType,
        dt: datetime.datetime = datetime.datetime.now(),
    ) -> None:
        """Update the last ingestion date for a label in the Metatable.

        Args:
            label (str, optional):
                The label that was ingested, or None if airtableid is provided.
                Defaults to None.
            ingestionType (IngestionUpdateType): The type of data that was ingested.
            dt (datetime.datetime, optional):
                The datetime to set the last ingestion date to.
                Defaults to datetime.datetime.now().

        Raises:
            ValueError: If both label and airtableid are None.
            ValueError: If label is given but a airtable record for that label could not be found.
            ValueError: If airtableid is given but is not a valid airtable id.
        """

        if label is None:
            raise ValueError("Must provide label")

        airtableid = self.label_airtableid_map.get(label, None)
        self.logger.info("Airtable record ID for label %s is %s", label, airtableid)

        if not airtableid:
            raise ValueError(f"Could not find airtable id for label: {label}")

        if not is_airtable_record_id(airtableid):
            raise ValueError(f"{airtableid} is not a valid airtable record id")

        column_to_update = self.ingestion_type_col_name_map.get(ingestionType, None)

        try:
            result = self.table.update(
                airtableid,
                {column_to_update: get_airtable_timestamp_str(dt)},
            )
        except HTTPError as e:
            self.logger.error("Error updating last ingestion date: %s", e)
            raise e
        self.logger.info(
            "Updated last ingestion date for label %s (Record ID: %s) to value: %s",
            label,
            airtableid,
            result["fields"][column_to_update],
        )


class Air2Neo:
    """Class for ingesting data from Airtable into Neo4j."""

    def __init__(
        self,
        /,
        airtable_api_key: str = environ.get("AIRTABLE_API_KEY", None),
        airtable_base_id: str = environ.get("AIRTABLE_BASE_ID", None),
        metatable_name: str = environ.get("AIRTABLE_METATABLE_NAME", "Metatable"),
        neo4j_uri: str = environ.get("NEO4J_URI", None),
        neo4j_username: str = environ.get("NEO4J_USERNAME", None),
        neo4j_password: str = environ.get("NEO4J_PASSWORD", None),
        metatable: Table = None,
        metatable_config: MetatableConfig = None,
        *,  # Only allow keyword arguments after this point
        neo4j_driver: GraphDatabase = None,
    ):
        """Create an Air2Neo instance. This class is the object that will run the ingestion process.

        Args:
            airtable_api_key (str, optional):
                The Airtable API key.
                Defaults to the value of the AIRTABLE_API_KEY environment variable.
            airtable_base_id (str, optional):
                The Airtable base id.
                Defaults to the value of the AIRTABLE_BASE_ID environment variable.
            metatable_name (str, optional):
                The name of the Metatable.
                Defaults to the value of the AIRTABLE_METATABLE_NAME environment variable, or
                "Metatable" if that environment variable is not set.
            neo4j_uri (str, optional):
                The Neo4j URI.
                Defaults to the value of the NEO4J_URI environment variable.
            neo4j_username (str, optional):
                The Neo4j username.
                Defaults to the value of the NEO4J_USERNAME environment variable.
            neo4j_password (str, optional):
                The Neo4j password.
                Defaults to the value of the NEO4J_PASSWORD environment variable.
            metatable (Table, optional):
                An Airtable Table object for the Metatable.
                Defaults to None.
            metatable_config (MetatableConfig, optional):
                The configuration for the Metatable.
                If not provided, the default configuration will be used.

        Raises:
            ValueError: If not enough information is provided to be able to create a neo4j driver.
        """
        # pylint: disable=too-many-arguments

        self.logger = _create_logger()

        # Validate Neo4j driver
        # Configure neo4j driver, if not already configured
        if not neo4j_driver and not (neo4j_uri and neo4j_username and neo4j_password):
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

        if metatable_config is not None:
            self.metatable_config = metatable_config
            return

        # MetatableConfig was not provided, so use whatever was provided to the Air2Neo object
        self.metatable_config = MetatableConfig(
            airtable_api_key=airtable_api_key,
            airtable_base_id=airtable_base_id,
            metatable_name=metatable_name,
            table=metatable,
        )

        self.downloaded_airtables_tup = []

    def run(self) -> None:
        """Run the ingestion process."""
        self.logger.info("Starting Airtable to Neo4j ingest job.")
        start_time = perf_counter()

        self.logger.info("Validating Airtable to Neo4j ingest job...")
        self.metatable_config.validate()
        self.logger.info("✨ Validation OK")

        self.create_indices_from_metatable()
        self.create_constraints_from_metatable()

        airtables = [
            Table(self.airtable_api_key, self.airtable_base_id, t)
            for t in self.metatable_config.label_airtableid_map.keys()
        ]
        self.logger.info("Found %s tables in Airtable: %s", len(airtables), airtables)

        # This part is in a for-loop because I wanted to convert this to a
        # concurrent job.
        self.downloaded_airtables_tup = []
        for label in airtables:
            self.downloaded_airtables_tup.append(self._download_airtable(label))

        downloaded_airtables_tup = self.downloaded_airtables_tup

        with self.neo4j_driver.session() as session:
            # Create Nodes

            self.logger.info("Creating translation ID mappings...")
            instructions = self.metatable_config.column_instructions
            translation_id_mapping = self._create_translation_id_mapping(
                downloaded_airtables_tup, instructions
            )

            for label, airtable_data in downloaded_airtables_tup:
                self.logger.info('Creating nodes for table "%s"...', label)
                instructions = self.metatable_config.column_instructions
                node_list = self._create_node_list(
                    airtable_data,
                    instructions[label],
                    id_mapping=translation_id_mapping,
                )

                with session.begin_transaction() as tx:
                    self.logger.info(
                        "Creating %s nodes for table %s...",
                        len(node_list),
                        label,
                    )
                    neo4jop_batch_create_nodes(
                        tx,
                        label=label,
                        node_list=node_list,
                        id_property=self.metatable_config.airtable_id_property_in_neo4j,
                    )
                    tx.commit()
                    self.metatable_config.update_last_ingestion_date(
                        label,
                        IngestionUpdateType.node_properties,
                        datetime.datetime.now(),
                    )
                    self.logger.info(
                        "Merged %s nodes for table %s.", len(node_list), label
                    )

            # Create Edges
            for label, airtable_data in downloaded_airtables_tup:
                self.logger.info('Creating edge dict for table "%s"...', label)
                unmapped_edge_list = self._create_edge_list(
                    airtable_data, instructions[label]
                )

                self.logger.info("Replacing Airtable IDs with translation IDs...")
                edge_list = self._map_edge_list_translation_id(
                    unmapped_edge_list, translation_id_mapping
                )

                with session.begin_transaction() as tx:
                    self.logger.info(
                        "Creating %s edges for table %s...",
                        len(edge_list),
                        label,
                    )
                    neo4jop_batch_create_edge(
                        tx,
                        edge_list=edge_list,
                        id_property=self.metatable_config.airtable_id_property_in_neo4j,
                    )
                    tx.commit()
                    self.metatable_config.update_last_ingestion_date(
                        label,
                        IngestionUpdateType.edges,
                        datetime.datetime.now(),
                    )
                    self.logger.info(
                        "Merged %s edges for table %s.", len(edge_list), label
                    )

        # Close driver
        self.neo4j_driver.close()

        # Print elapsed time
        self.logger.info(
            "Airtable to Neo4j ingest job completed in %0.2f seconds.",
            perf_counter() - start_time,
        )

    def _create_translation_id_mapping(
        self,
        downloaded_airtables_tup: Sequence[Tuple[str, Sequence[Dict[str, Any]]]],
        instructions: Dict[str, Dict[str, str]],
    ) -> Dict[str, str]:
        id_mapping = {}

        for label, data in downloaded_airtables_tup:
            instruction = instructions[label]
            translation_id_col = instruction["TranslationId"]

            if translation_id_col is None:
                continue

            for record in data:
                if "TranslationId" not in instruction:
                    self.logger.warning("No TranslationId column found for %s", label)

                if translation_id_col not in record["fields"]:
                    self.logger.warning(
                        "No TranslationId value found for %s, id column: %s",
                        label,
                        translation_id_col,
                    )

                id_mapping[record["id"]] = record["fields"][translation_id_col]

        return id_mapping

    def _map_edge_list_translation_id(
        self,
        edge_list: Sequence[List[str]],
        translation_id_mapping: Dict[str, str],
    ) -> List[List[str]]:
        """Replace all occurrences of the Airtable ID with the Translation ID, given
        the translation ID mapping.

        Args:
            edge_list (Sequence[Tuple]): List of edges to be mapped.
            translation_id_mapping (Dict[str, str]): Mapping of Airtable ID to Translation ID.

        Returns:
            List[Tuple]: List of edges with the Airtable ID replaced with the Translation ID.
        """

        edge_list = [
            [
                translation_id_mapping.get(source_id, source_id),
                translation_id_mapping.get(target_id, target_id),
                edge_name,
            ]
            for (source_id, target_id, edge_name) in edge_list
        ]

        return edge_list

    def _download_airtable(self, table: Table) -> Tuple[str, DataFrame]:
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
        downloaded_table = table.all()
        self.logger.info(
            "Downloaded Airtable table %s (Records: %s) in %0.2f seconds",
            name,
            len(downloaded_table),
            perf_counter() - start_time,
        )

        return name, downloaded_table

    def _create_node_list(
        self,
        airtable_data: Sequence[Dict],
        instructions: Dict[str, List[str]],
        id_mapping: Dict[str, str] = None,
    ) -> List[Dict]:
        """Creates a list of nodes from a single Airtable table.

        Args:
            airtable_data (Sequence[Dict]):
                A list of dictionaries, where each dictionary represents a row
                in the Airtable table.
            instructions (Dict[str, List[str]]):
                A dictionary containing instructions for how to create the
                nodes. The keys are the instructions to perform, and the values
                are the columns to perform the operation on.

        Returns:
            List[Dict]:
                A list of dictionaries, where each dictionary represents a node
                to be created in Neo4j.
        """

        def create_node_dict(
            record: Dict, node_property_columns: Sequence[str]
        ) -> Dict:
            d = {
                k: v for k, v in record["fields"].items() if k in node_property_columns
            }
            d[self.metatable_config.airtable_id_property_in_neo4j] = id_mapping.get(
                record["id"], record["id"]
            )
            return d

        node_property_columns = instructions["NodeProperties"]
        node_list = [
            create_node_dict(record, node_property_columns) for record in airtable_data
        ]

        return node_list

    def _create_edge_list(
        self, airtable_data: Sequence[Dict], instruction: Dict[str, str]
    ) -> List[List[str]]:
        """Creates a list of edges from a single Airtable table.

        Args:
            airtable_data (Sequence[Dict]):
                A list of dictionaries, where each dictionary represents a row
                in the Airtable table.
            instruction (Dict):
                A dictionary containing instructions for how to create the
                edges. The keys are the instructions to perform, and the values
                are the columns to perform the operation on.

        Returns:
            List[List[str]]:
                A list of list of strings, where each inner list represents an edge to
                be to be created in Neo4j. The tuple format is:
                (source_id, target_id, edge_name)
        """
        edges_columns = instruction["Edges"]
        edge_list = []
        for record in airtable_data:
            for edge_name in edges_columns:
                edge_name_formatted = self.metatable_config.format_edge_col_name(
                    edge_name
                )
                if edge_name in record["fields"]:
                    for target_id in record["fields"][edge_name]:
                        edge_list.append([record["id"], target_id, edge_name_formatted])

        return edge_list

    def create_indices_from_metatable(self) -> None:
        """Creates indices from the metatable."""
        with self.neo4j_driver.session() as session:
            for (
                label,
                instructions,
            ) in self.metatable_config.column_instructions.items():
                self.logger.info("Creating indices for label %s", label)
                index_for_columns = instructions["IndexFor"]
                if len(index_for_columns) > 0:
                    with session.begin_transaction() as tx:
                        neo4jop_create_index_for_label(tx, label, index_for_columns)
                        tx.commit()
                        self.logger.info("Created indices for label %s", label)
                else:
                    self.logger.info("No indices to create for label %s", label)

    def create_constraints_from_metatable(self) -> None:
        """Creates constraints for the Neo4j label from the Airtable meta-table."""
        with self.neo4j_driver.session() as session:
            for (
                label,
                instructions,
            ) in self.metatable_config.column_instructions.items():
                self.logger.info("Creating constraints for label %s", label)
                constraint_for_columns = instructions["ConstrainFor"]
                if len(constraint_for_columns) > 0:
                    with session.begin_transaction() as tx:
                        neo4jop_create_constraint_for_label(
                            tx, label, constraint_for_columns
                        )
                        tx.commit()
                        self.logger.info("Created constraints for label %s", label)
                else:
                    self.logger.info("No constraints to create for label %s", label)
