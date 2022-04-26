from typing import Any, Dict, Sequence

from neo4j import Transaction

from .config import airtable_id_col, edge_source, edge_target, edge_type


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
    result = tx.run(cypher_query)
    return result


def batch_create_node(tx: Transaction,
                      label: str,
                      nodes_list: Sequence[Dict[str, Any]]):
    cypher_query = f'''UNWIND $nodes_list AS node
MERGE (n:{label} {{{airtable_id_col}: node.{airtable_id_col}}})
SET n = node'''
    result = tx.run(cypher_query, nodes_list=nodes_list)


def batch_create_edge(tx: Transaction,
                      edges_list: Sequence[Dict[str, str]]):
    cypher_query = f'''UNWIND $edges_list AS edge
MATCH (n) WHERE n.{airtable_id_col} = edge.{edge_source}
MATCH (m) WHERE m.{airtable_id_col} = edge.{edge_target}
MERGE (n)-[r:edge.`{edge_type}`]->(m)'''
    result = tx.run(cypher_query, edges_list=edges_list)
