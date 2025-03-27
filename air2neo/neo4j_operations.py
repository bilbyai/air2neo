import logging
from typing import Any, Dict, List, Sequence

from neo4j import Result, Session, Transaction


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
    session: Session,
    # tx: Transaction,
    edge_list: Sequence[List[str]],
    log: logging.Logger,
    *,
    id_property: str = "_aid",
    batch_size: int = 50,  # Batch size must be small if using Neo4J Aura Free Tier
    parallel: bool = False,  # Can't run jobs in parallel in order for _counters to be accurate
    iterateList: bool = True,
    retries: int = 10,
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

    # Recreate the _counters nodes in an earlier, separate transaction.
    with session.begin_transaction() as tx:
        # Delete any stale _counters nodes
        tx.run("MATCH (c:_counters) DELETE c")

        # Create a new _counters node to store the count
        tx.run(
            "CREATE (c:_counters {sources_not_found: 0, targets_not_found: 0, edges_created: 0, edges_skipped: 0})"
        )
        tx.commit()

    cypher = (
        f"CALL apoc.periodic.iterate("
        f'"UNWIND $edge_list AS edge RETURN edge",'
        f'"WITH edge[0] AS source_id, edge[1] AS target_id, edge[2] AS edge_type '
        f" OPTIONAL MATCH (n) WHERE n.{id_property} = source_id "
        f" OPTIONAL MATCH (m) WHERE m.{id_property} = target_id "
        f" OPTIONAL MATCH (n)-[r]->(m) "
        f" WITH n, m, r, edge_type, "
        f"      CASE "
        f"         WHEN type(r) = edge_type THEN True "
        f"         ELSE False "
        f"      END as relationExists "
        f" CALL apoc.do.when("
        f"   n IS NULL,"
        f"  'MATCH (c:_counters) SET c.sources_not_found = c.sources_not_found + 1 RETURN {{source_not_found: true}}',"
        f"   '', {{}}) YIELD value as sourceNotFound "
        f" CALL apoc.do.when("
        f"   m IS NULL,"
        f"  'MATCH (c:_counters) SET c.targets_not_found = c.targets_not_found + 1 RETURN {{target_not_found: true}}',"
        f"   '', {{}}) YIELD value as targetNotFound "
        f" CALL apoc.do.when("
        f"   relationExists,"
        f"  'MATCH (c:_counters) SET c.edges_skipped = c.edges_skipped + 1 RETURN {{edge_skipped: true}}',"
        f"   'MATCH (n_rebound) WHERE id(n_rebound) = id(n) "
        f"    MATCH (m_rebound) WHERE id(m_rebound) = id(m) "
        f"    CALL apoc.create.relationship(n_rebound, edge_type, {{}}, m_rebound) YIELD rel "
        f"    MATCH (c:_counters) SET c.edges_created = c.edges_created + 1 RETURN {{edge_created: true}}',"
        f"   {{n: n, m: m, edge_type: edge_type}}) YIELD value "
        f' RETURN value",'
        f"{{batchSize: {batch_size}, "
        f"parallel: {str(parallel).lower()}, "
        f"iterateList: {str(iterateList).lower()}, "
        f"retries: {retries}, "
        f"params: {{edge_list: $edge_list}}}})"
        f"YIELD batches, total, errorMessages, failedBatches "
        f"RETURN batches, total, errorMessages, failedBatches"
    )

    # Now, execute the main transaction, using the _counters Node.
    with session.begin_transaction() as tx:
        res = tx.run(cypher, edge_list=edge_list)
        res_single = res.single()

        # Query the count node for the number of created relationships
        count_result = tx.run(
            "MATCH (c:_counters) "
            "RETURN c.edges_created as num_edges_created, "
            "c.edges_skipped as num_edges_skipped, "
            "c.sources_not_found as sources_not_found, "
            "c.targets_not_found as targets_not_found"
        )
        count_record = count_result.single()

        # Delete the _counters node
        tx.run("MATCH (c:_counters) DELETE c")
        tx.commit()

    # Log the final status.
    log.info(
        {
            "batches": res_single["batches"],
            "total": res_single["total"],
            "errorMessages": res_single["errorMessages"],
            "failedBatches": res_single["failedBatches"],
            "num_edges_created": count_record["num_edges_created"],
            "num_edges_skipped": count_record["num_edges_skipped"],
            "sources_not_found": count_record["sources_not_found"],
            "targets_not_found": count_record["targets_not_found"],
        }
    )

    return res
