from typing import Any


def is_airtable_record_id(record: Any) -> bool:
    ''' Checks if a single record is an airtable ID.
    An airtable ID is defined by 3 things:
    1. It is a string.
    2. All characters are alphanumeric.
    3. It is a string of length 17
    4. It starts with 'rec'

    Args:
        record (Any): A single record.

    Returns:
        bool: Returns true if the record is an airtable ID, and false if it is
        not.
    '''
    return all((
        isinstance(record, str),
        record.isalnum(),
        len(record) == 17,
        record.startswith("rec")
    ))
