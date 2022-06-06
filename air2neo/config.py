def keep_col_rule_default(column_name: str) -> bool:
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


def is_edge_rule_default(column_name: str) -> bool:
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


def is_prop_rule_default(column_name: str) -> bool:
    ''' Checks if a column name is a node property column.

    Args:
        column_name (str): The name of the column.

    Returns:
        bool: Returns true if the column is a node property column, and false
        if it is an edge column.
    '''
    if not isinstance(column_name, str):
        return False
    return not is_edge_rule_default(column_name)


def format_edge_col_name_default(col: str) -> str:
    ''' Formats an edge column name.
    Anything after a dunder (double underline) is removed.

    Args:
        col (str): The name of the column.

    Returns:
        str: The formatted column name.
    '''
    return col.split('__')[0]
