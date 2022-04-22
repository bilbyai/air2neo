from neo4j import GraphDatabase


class Neo4jManager(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def add_node(self, label: str, properties: dict, *, id: str = None):
        """Add a Node to the graph.

        Args:
            label (str): The label of the node, i.e. the type of entity.
            properties (dict): The properties of the node.
            uuid (str, optional): The ID of the node. Defaults to None. Format follows
            Airtable's ID format (e.g. 'recPlij4GY0EFchu2', 17 characters long,
            starts with 'rec', the rest is alphanumeric).

        Returns:
            str: The ID of the node.
        """
        with self._driver.session() as session:
            return session.write_transaction(self._add_node, label, properties)
    
    def _add_node(self, tx, label: str, properties: dict, id: str = None):
        if id is None:
            id = tx.run("CREATE (n:%s {properties}) RETURN id(n)" % label,
                        properties=properties).single()[0]
        else:
            tx.run("CREATE (n:%s {properties}) SET n.id = $id" % label,
                   properties=properties, id=id)
        return id

    # def add_node(self, label, properties):
    #     with self._driver.session() as session:
    #         session.write_transaction(self._add_node, label, properties)

    # def _add_node(self, tx, label, properties):
    #     tx.run("CREATE (a:$label {$properties})",
    #            label=label, 
    #            properties=properties)

    # def add_relationship(self, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties):
    #     with self._driver.session() as session:
    #         session.write_transaction(self._add_relationship, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties)

    # def _add_relationship(self, tx, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties):
    #     tx.run("MATCH (a:$start_label {$start_properties}), (b:$end_label {$end_properties}) CREATE (a)-[r:$relationship_type {$relationship_properties}]->(b)",
    #         start_label=start_label, start_properties=start_properties, end_label=end_label, end_properties=end_properties, relationship_type=relationship_type, relationship_properties=relationship_properties)

    # def get_node(self, label, properties):
    #     with self._driver.session() as session:
    #         return session.write_transaction(self._get_node, label, properties)

    # def _get_node(self, tx, label, properties):
    #     return tx.run("MATCH (a:$label {$properties}) RETURN a", label=label, properties=properties).single()

    # def get_relationship(self, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties):
    #     with self._driver.session() as session:
    #         return session.write_transaction(self._get_relationship, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties)

    # def _get_relationship(self, tx, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties):
    #     return tx.run("MATCH (a:$start_label {$start_properties})-[r:$relationship_type {$relationship_properties}]->(b:$end_label {$end_properties}) RETURN r",
    #         start_label=start_label, start_properties=start_properties, end_label=end_label, end_properties=end_properties, relationship_type=relationship_type, relationship_properties=relationship_properties).single()

    # def get_relationships(self, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties):
    #     with self._driver.session() as session:
    #         return session.write_transaction(self._get_relationships, start_label, start_properties, end_label, end_properties, relationship_type, relationship_properties)
    
