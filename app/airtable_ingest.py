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

    for table in tables:
        label = table # neo4j label = table name
        table_airtable = Table(api_key, base_id, table)
        df = pd.DataFrame(table_airtable.all())

        df = df.apply(_split_node_edge, axis=1)

        for idx, row in df.iterrows():
            id = row['id']
            props = row['props']
            edges = row['edges']

            # Create node
            # TODO

            # Create edges
            # TODO


