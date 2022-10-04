def format_edge_col_name_default(col: str) -> str:
    """Formats an edge column name.
    Anything after a dunder (double underline) is removed.

    Args:
        col (str): The name of the column.

    Returns:
        str: The formatted column name.
    """
    return col.split("__")[0]
