import json
import os
from typing import Any

from neo4j import GraphDatabase
from pandas import DataFrame, Series
from pyairtable import Table

from .vars import airtable_id_col, airtable_ref_table

AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']
AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']

NEO4J_USERNAME = os.environ['NEO4J_USERNAME']
NEO4J_PASSWORD = os.environ['NEO4J_PASSWORD']
NEO4J_URI = os.environ['NEO4J_URI']


def keep_col_cond(column_name: str) -> bool:
    ''' Checks if a column name should be kept.

    Args:
        column_name (str): The name of the column.

    Returns:
        bool: Returns true if the column should be kept, and false if it should
        be discarded.
    '''
    if not isinstance(column_name, str):
        return False
    return not column_name.startswith('_')


def edge_col_cond(column_name: str) -> bool:
    ''' Checks if a column name is an edge column or a node property column.

    Args:
        column_name (str): The name of the column.

    Returns:
        bool: Returns true if the column is an edge column, and false if it is
        a node property column.
    '''
    if not isinstance(column_name, str):
        return False
    return column_name.isupper()


def prop_col_cond(column_name: str) -> bool:
    ''' Checks if a column name is a node property column.

    Args:
        column_name (str): The name of the column.

    Returns:
        bool: Returns true if the column is a node property column, and false if
        it is an edge column.
    '''
    if not isinstance(column_name, str):
        return False
    return not edge_col_cond(column_name)


def format_edge_col(col: str) -> str:
    ''' Formats an edge column name.
    Anything after a dunder (double underline) is removed.

    Args:
        col (str): The name of the column.

    Returns:
        str: The formatted column name.
    '''
    return col.split('__')[0]


def is_airtable_record_id(record: Any) -> bool:
    ''' Checks if a single record is an airtable ID.
    An airtable ID is defined by 3 things:
    1. It is a string.
    2. All characters are alphanumeric.
    3. It is a string of length 17
    4. It starts with 'rec'

    Args:
        record (Any): A single record.

    Returns:
        bool: Returns true if the record is an airtable ID, and false if it is
        not.
    '''
    return all((
        isinstance(record, str),
        record.isalnum(),
        len(record) == 17,
        record.startswith("rec")
    ))


def _split_node_edge(row: Series) -> Series:
    # This function is created just to do a df.apply()
    row['fields'] = {k: v
                     for k, v in row['fields'].items()
                     if keep_col_cond(k)}

    row['edges'] = {format_edge_col(k):v
                    for k, v in row['fields'].items()
                    if edge_col_cond(k)}

    row['props'] = {k: v
                    for k, v in row['fields'].items()
                    if prop_col_cond(k)}

    del row['fields']

    if row['createdTime']:
        del row['createdTime']

    return row


def start_ingest():
    '''
        This function is used to ingest data from Airtable into Neo4j.
    '''

    # Retrieve the Airtable reference table
    ref_table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, airtable_ref_table)
    tables = [x['fields']['Name'] for x in ref_table.all()]
    airtables = [Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, t) for t in tables]
    dataframes = [DataFrame(t.all()) for t in airtables]

    # Split Nodes and Edges
    dataframes = [df.apply(_split_node_edge, axis=1) for df in dataframes]

    driver = GraphDatabase.driver(NEO4J_URI,
                                  auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    with driver.session() as session:
        # Create Nodes
        for table, df in zip(tables, dataframes):
            for _, row in df.iterrows():
                id, props, edges = row['id'], row['props'], row['edges']

                cypher = [f'MERGE (n:{table} {{{airtable_id_col}: "{id}"}})']

                for k,v in props.items():
                    if isinstance(v, (int, float, bool)):
                        # if is non-string primitive type
                        cypher.append(f'SET n.`{k}` = "{v}"')

                    elif isinstance(v, str):
                        # if is string
                        v = v.replace('"', '\\"')
                        cypher.append(f'SET n.`{k}` = "{v}"')

                    elif all((isinstance(v, list), all(isinstance(x, str) for x in v))):
                        # if is string list
                        v = [v.replace('"', '\\"') for v in v]
                        cypher.append(f'SET n.`{k}` = {v}')

                    else:
                        # dump as JSON
                        v = json.dumps(v).replace('"', '\\"')
                        cypher.append(f'SET n.`{k}` = "{v}"')

                cypher = '\n'.join(cypher)

                session.run(cypher)

        # Create Edges
        for table, df in zip(tables, dataframes):
            for _, row in df.iterrows():
                id, props, edges = row['id'], row['props'], row['edges']
                for k,v in edges.items():
                    for v_ in v:
                        cypher = []
                        cypher.append(f'MATCH (n) WHERE n.{airtable_id_col} = "{id}"')
                        cypher.append(f'MATCH (m) WHERE m.{airtable_id_col} = "{v_}"')
                        cypher.append(f'MERGE (n)-[r:`{k}`]->(m)')
                        cypher = '\n'.join(cypher)
                        session.run(cypher)

    driver.close()
