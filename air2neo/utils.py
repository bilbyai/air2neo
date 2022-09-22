import datetime
from typing import Any


def is_airtable_record_id(record: Any) -> bool:
    """Checks if a single record is an airtable ID.
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
    """
    return (
        isinstance(record, str)
        and record.isalnum()
        and len(record) == 17
        and record.startswith("rec")
    )


def get_airtable_timestamp_str(dt: datetime.datetime) -> str:
    """Converts a datetime object to an airtable-friendly timestamp string.
    Returns a timestamp in the following format: '2022-09-20T06:08:51.601Z'

    Args:
        dt (datetime.datetime): A datetime object.

    Returns:
        str: A timestamp string.
    """
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
