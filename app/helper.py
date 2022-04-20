from pandas import Series


def is_airtable_link(record):
    if isinstance(record, str):
        return record.startswith("rec")
    elif isinstance(record, (list, Series, tuple)):
        return all(r.startswith("rec") for r in record)
    return False
