from typing import Sequence

from neo4j import Transaction


def create_node(tx: Transaction, label: str, properties: dict) -> None:
    ''' Creates a node in the database.

    Args:
        tx (Transaction): The transaction to use.
        label (str): The label of the node.
        properties (dict): The properties of the node.
    '''
    tx.create(
        f"({label} {{{', '.join(f'{k}: {v}' for k, v in properties.items())}}})",
    )

def create_index_for(tx: Transaction, label: str, indexes: Sequence[str]) -> None:
    pass
