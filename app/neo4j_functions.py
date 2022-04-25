from typing import Any, Dict, Sequence

from neo4j import Transaction

from .config import airtable_id_col


def create_node(tx: Transaction, label: str, properties: dict):
    ''' Creates a node in the database.

    Args:
        tx (Transaction): The transaction to use.
        label (str): The label of the node.
        properties (dict): The properties of the node.
    '''
    tx.create(
        f"({label} {{{', '.join(f'{k}: {v}' for k, v in properties.items())}}})",
    )

def create_index_for(tx: Transaction,
                     label: str,
                     indexes: Sequence[str]):
    index_query = ', '.join([f'n.{index}' for index in indexes])
    cypher_query = f'CREATE INDEX IF NOT EXISTS FOR (n.{label}) ON ({index_query})'
    tx.run(cypher_query)
    # TODO not sure if this is the right way to code this
    return tx.consume()

def batch_create_node(tx: Transaction,
                      label: str,
                      nodes_list: Sequence[Dict[str, Any]]):
    cypher_query = 'UNWIND $nodes_list AS node'
    cypher_query += f'MERGE (n:{label}) WHERE n.{airtable_id_col} = node.{airtable_id_col}'
    cypher_query += 'SET n = node'
    tx.run(cypher_query, nodes_list=nodes_list)
    # TODO not sure if this is the right way to code this
    return tx.consume()
