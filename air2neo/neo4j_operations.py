from typing import Any, Dict, Sequence, Tuple

from neo4j import Result, Transaction


def neo4jop_batch_create_nodes(
    tx: Transaction,
    label: str,
    node_list: Sequence[Dict[str, Any]],
    *,
    id_property: str = "_aid",
) -> Result:
    """Creates a batch of nodes.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label of the nodes.
        node_list (Sequence[Dict[str, Any]]): The list of nodes to create.
    """
    cypher = (
        f"UNWIND $node_list AS node "
        f"MERGE (n:`{label}` {{ {id_property}: node.{id_property} }}) "
        f"SET n = node"
    )
    res = tx.run(cypher, node_list=node_list)
    return res


def neo4jop_create_constraint_for_label(
    tx: Transaction, label: str, constraint: str
) -> Result:
    """Creates a constraint for a label.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label to create a constraint for.
        constraint (str): The constraint to create.
        log (Any, optional): The logger to use. Defaults to logger.
    """
    constraint_properties = ", ".join([f"n.`{prop}`" for prop in constraint])
    cypher = (
        f"CREATE CONSTRAINT IF NOT EXISTS "
        f"FOR (n:`{label}`)"
        f"REQUIRE ({constraint_properties}) IS UNIQUE"
    )
    res = tx.run(cypher)
    return res


def neo4jop_create_index_for_label(
    tx: Transaction, label: str, indexes: Sequence[str]
) -> Result:
    """Creates an index for a label.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label to create an index for.
        indexes (Sequence[str]): The indexes to create.

    Returns:
        Result: The result of the query.
    """
    index_query = ", ".join([f"n.`{index}`" for index in indexes])
    cypher = f"CREATE INDEX IF NOT EXISTS FOR (n:`{label}`) ON ({index_query})"
    res = tx.run(cypher)
    return res


def neo4jop_batch_create_edge(
    tx: Transaction,
    edge_list: Sequence[Tuple[str, str, str]],
    *,
    id_property: str = "_aid",
) -> Result:
    """Creates a batch of edges.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        edge_list (Sequence[Tuple[str, str, str]]): The list of edges to create.
        The tuple format is:
            (source_id, target_id, edge_label)
            Example: ('recSOURCEXXXXXX', 'recTARGETXXXXX', 'IN_INDUSTRY')
        log (Any, optional): The logger to use. Defaults to logger.
    """
    cypher = (
        f"UNWIND $edge_list AS edge "
        f"MATCH (n) WHERE n.{id_property} = edge[0] "
        f"MATCH (m) WHERE m.{id_property} = edge[1] "
        f"OPTIONAL MATCH (n)-[rel]-(m) "
        f"WITH n, m, edge, COLLECT(TYPE(rel)) AS relTypes "
        f"WHERE NOT edge[2] IN relTypes "
        f"CALL apoc.create.relationship(n, edge[2], NULL, m) "
        f"YIELD rel "
        f"RETURN 0"
    )
    res = tx.run(cypher, edge_list=edge_list)
    return res
