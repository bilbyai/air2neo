from typing import Any, Dict, Sequence

from neo4j import Transaction

from .config import airtable_id_col, edge_label, edge_source, edge_target, logger


def create_index_for(tx: Transaction,
                     label: str,
                     indexes: Sequence[str],
                     *,
                     log: Any = logger):
    """ Creates an index for a label.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label to create an index for.
        indexes (Sequence[str]): The indexes to create.
        log (Any, optional): The logger to use. Defaults to logger.

    Returns:
        _type_: _description_
    """

    index_query = ', '.join([f'n.{index}' for index in indexes])
    cypher = f'''
CREATE INDEX IF NOT EXISTS FOR (n.{label})
ON ({index_query})'''

    log.info('Creating indexes for %s', label)
    res = tx.run(cypher)
    log.info('Created indexes for %s', label)

    return res


def create_constraint_for(tx: Transaction,
                          label: str,
                          constraint: str,
                          *,
                          log: Any = logger):
    """ Creates a constraint for a label.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label to create a constraint for.
        constraint (str): The constraint to create.
        log (Any, optional): The logger to use. Defaults to logger.
    """

    cypher = f'''
CREATE CONSTRAINT IF NOT EXISTS ON (n:{label})
ASSERT n.{constraint} IS UNIQUE'''

    log.info('Creating constraint for %s', label)
    res = tx.run(cypher)
    log.info('Created constraint for %s', label)

    return res


def batch_create_node(tx: Transaction,
                      label: str,
                      node_list: Sequence[Dict[str, Any]],
                      *,
                      log: Any = logger):
    """ Creates a batch of nodes.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label of the nodes.
        node_list (Sequence[Dict[str, Any]]): The list of nodes to create.
        log (Any, optional): The logger to use. Defaults to logger.
    """

    cypher = f'''
UNWIND $node_list AS node
MERGE (n:{label} {{{airtable_id_col}: node.{airtable_id_col}}})
SET n = node'''

    log.info('Creating nodes for %s', label)
    res = tx.run(cypher, node_list=node_list)
    log.info('Created nodes for %s', label)

    return res


def batch_create_edge(tx: Transaction,
                      edge_list: Sequence[Dict[str, str]],
                      *,
                      log: Any = logger):
    """ Creates a batch of edges.

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

    cypher = f'''
UNWIND $edge_list AS edge
MATCH (n) WHERE n.{airtable_id_col} = edge.{edge_source}
MATCH (m) WHERE m.{airtable_id_col} = edge.{edge_target}
OPTIONAL MATCH (n)-[rel]-(m)
WITH n, m, edge, COLLECT(TYPE(rel)) AS relTypes
WHERE NOT edge.{edge_label} IN relTypes
CALL apoc.create.relationship(n, edge.{edge_label}, NULL, m)
YIELD rel
RETURN n, m, rel'''

    log.info('Creating edges')
    res = tx.run(cypher, edge_list=edge_list)
    log.info('Created edges')

    return res
