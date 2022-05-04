
from time import perf_counter

from neo4j import GraphDatabase
from pandas import DataFrame, Series
from pyairtable import Table

from .config import (AIRTABLE_API_KEY, AIRTABLE_BASE_ID, NEO4J_PASSWORD, NEO4J_URI,
                     NEO4J_USERNAME, airtable_id_col, airtable_ref_table, edge_label,
                     edge_source, edge_target, format_edge_col_name, is_edge_rule,
                     is_prop_rule, keep_col_rule, logger)
from .neo4j_functions import batch_create_edge, batch_create_node, create_constraint_for


def _split_node_edge(row: Series) -> Series:
    # This function is created just to do a df.apply()
    row['fields'] = {k: v
                     for k, v in row['fields'].items()
                     if keep_col_rule(k)}

    row['edges'] = {format_edge_col_name(k): v
                    for k, v in row['fields'].items()
                    if is_edge_rule(k)}

    row['props'] = {k: v
                    for k, v in row['fields'].items()
                    if is_prop_rule(k)}

    del row['fields']

    if row['createdTime']:
        del row['createdTime']

    return row


def run_airtable_to_neo4j_ingest_job(*, nuke: bool = False) -> None:
    '''
        This function is used to ingest data from Airtable into Neo4j.

        Args:
            nuke (bool): If true, the Neo4j database will be nuked before
            ingesting data.

        Returns:
            None: If everything was OK, it returns None.
    '''
    logger.info("Starting Airtable to Neo4j ingest job.")
    start_time = perf_counter()

    # Retrieve the Airtable reference table
    ref_table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, airtable_ref_table)
    tables = [x['fields']['Name'] for x in ref_table.all()]

    logger.info('Found %s tables in Airtable: %s', len(tables), tables)

    airtables = [Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, t) for t in tables]

    def _download_airtable_and_return_as_df(table: Table) -> DataFrame:
        ''' Downloads a single Airtable table and returns it as a DataFrame.

        Args:
            table (Table): A single Airtable table.

        Returns:
            DataFrame: A DataFrame containing the data from the Airtable table.
        '''
        name = table.table_name
        logger.info('Downloading Airtable table %s', name)
        start_time = perf_counter()
        df = DataFrame(table.all())
        logger.info('Downloaded Airtable table %s (Records: %s) in %s seconds',
                    name, len(df), perf_counter() - start_time)
        df = df.apply(_split_node_edge, axis=1)
        return name, df

    # dfs = []
    # with ThreadPoolExecutor() as executor:
    #     dfs = executor.map(_download_airtable_and_return_as_df, airtables)

    # print([f'{n}: {len(df)}' for n, df in dfs])

    dfs = []
    for table in airtables:
        dfs.append(_download_airtable_and_return_as_df(table))

    del airtables

    logger.info('Creating Neo4j driver...')
    driver = GraphDatabase.driver(NEO4J_URI,
                                  auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

    logger.info('Creating Neo4j session...')
    with driver.session() as session:

        if nuke:
            logger.info('`nuke` is set to True. Nuking Neo4j database...')
            session.run('MATCH (n) DETACH DELETE n')

        # Create Nodes
        for table, df in dfs:
            logger.info('Creating Constraint for table "%s"...', table)

            logger.info('Creating nodes for table "%s"...', table)

            def make_node_list(row):
                node = row['props']
                node[airtable_id_col] = row['id']
                return node

            node_list = df.apply(make_node_list, axis=1).to_list()

            with session.begin_transaction() as tx:
                logger.info('Creating %s nodes for table "%s"...',
                            len(node_list), table)

                batch_create_node(tx, label=table, node_list=node_list)

                logger.info('%s nodes created/merged for table "%s".',
                            len(node_list), table)

                tx.commit()
                tx.close()

            with session.begin_transaction() as tx:
                # Create constraint
                logger.info('Creating constraint for table "%s"...', table)
                create_constraint_for(tx,
                                      label=table,
                                      constraint=airtable_id_col)

                tx.commit()
                tx.close()

        # Create Edges
        for table, df in dfs:
            edge_list = []
            for _, row in df.iterrows():
                id, edges = row['id'], row['edges']
                for k, v in edges.items():
                    for v_ in v:
                        edge = {}
                        edge[edge_source] = id
                        edge[edge_target] = v_
                        edge[edge_label] = k
                        edge_list.append(edge)

            logger.info('Creating %s edges...', len(edge_list))
            # session.write_transaction(batch_create_edge, edge_list)
            with session.begin_transaction() as tx:
                batch_create_edge(tx, edge_list=edge_list)
                tx.commit()
                tx.close()

            logger.info('%s edges created/merged for table "%s".',
                        len(edge_list), table)

        driver.close()

    # TODO something is wrong with the timer
    logger.info('Ingestion completed in %s seconds.', perf_counter() - start_time)
