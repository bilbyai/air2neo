import logging
from typing import Any, Dict, List, Optional, Sequence

from neo4j import Result, Session, Transaction


def _quote_cypher_identifier(identifier: str) -> str:
    """Quote a Cypher identifier, escaping embedded backticks."""
    return f"`{identifier.replace('`', '``')}`"


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
    label_identifier = _quote_cypher_identifier(label)
    id_property_identifier = _quote_cypher_identifier(id_property)
    cypher = (
        f"UNWIND $node_list AS node "
        f"MERGE (n:{label_identifier} {{ {id_property_identifier}: "
        f"node.{id_property_identifier} }}) "
        f"SET n = node"
    )
    res = tx.run(cypher, node_list=node_list)
    return res


def neo4jop_create_constraint_for_label(
    tx: Transaction, label: str, constraint: Sequence[str]
) -> Result:
    """Creates a constraint for a label.

    Args:
        tx (Transaction): The Neo4j transaction to use.
        label (str): The label to create a constraint for.
        constraint (str): The constraint to create.
        log (Any, optional): The logger to use. Defaults to logger.
    """
    label_identifier = _quote_cypher_identifier(label)
    constraint_properties = ", ".join(
        [f"n.{_quote_cypher_identifier(prop)}" for prop in constraint]
    )
    cypher = (
        f"CREATE CONSTRAINT IF NOT EXISTS "
        f"FOR (n:{label_identifier})"
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
    label_identifier = _quote_cypher_identifier(label)
    index_query = ", ".join(
        [f"n.{_quote_cypher_identifier(index)}" for index in indexes]
    )
    cypher = f"CREATE INDEX IF NOT EXISTS FOR (n:{label_identifier}) ON ({index_query})"
    res = tx.run(cypher)
    return res


def neo4jop_batch_create_edge(
    session: Session,
    edge_list: Sequence[List[str]],
    log: Optional[logging.Logger] = None,
    *,
    id_property: str = "_aid",
    batch_size: int = 1000,
) -> Optional[Result]:
    """Creates a batch of edges.

    Args:
        session (Session): The Neo4j session to use.
        edge_list (Sequence[Tuple[str, str, str]]): The list of edges to create.
        The tuple format is:
            (source_id, target_id, edge_label)
            Example: ('recSOURCEXXXXXX', 'recTARGETXXXXX', 'IN_INDUSTRY')
        log (Any, optional): The logger to use. Defaults to logger.
    """
    if not edge_list:
        if log:
            log.info("No edges to merge.")
        return None

    id_property_identifier = _quote_cypher_identifier(id_property)
    cypher = (
        "UNWIND $edge_list AS edge "
        "WITH edge[0] AS source_id, edge[1] AS target_id, edge[2] AS edge_type "
        f"OPTIONAL MATCH (source) WHERE source.{id_property_identifier} = source_id "
        f"OPTIONAL MATCH (target) WHERE target.{id_property_identifier} = target_id "
        "WITH source, target, edge_type "
        "FOREACH (_ IN CASE "
        "WHEN source IS NULL OR target IS NULL THEN [] ELSE [1] END | "
        "MERGE (source)-[:$(edge_type)]->(target)) "
        "RETURN count(*) AS total, "
        "sum(CASE WHEN source IS NULL THEN 1 ELSE 0 END) AS sources_not_found, "
        "sum(CASE WHEN target IS NULL THEN 1 ELSE 0 END) AS targets_not_found, "
        "sum(CASE WHEN source IS NOT NULL AND target IS NOT NULL THEN 1 ELSE 0 END) "
        "AS relationships_processed"
    )

    stats = {
        "total": 0,
        "sources_not_found": 0,
        "targets_not_found": 0,
        "relationships_processed": 0,
    }
    res = None
    for start in range(0, len(edge_list), batch_size):
        batch = edge_list[start : start + batch_size]
        with session.begin_transaction() as tx:
            res = tx.run(cypher, edge_list=batch)
            record = res.single()
            tx.commit()

        if record:
            for key in stats:
                stats[key] += record[key] or 0

    if log:
        log.info(stats)

    return res
