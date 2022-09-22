from air2neo import utils


def test_is_airtable_record_id_utils():
    assert utils.is_airtable_record_id("rec12345678901234") is True
    assert utils.is_airtable_record_id("recycledblackjack") is True
    assert utils.is_airtable_record_id("rec!!!!!!!!!!!!!!") is False
    assert utils.is_airtable_record_id("rec123456789012345") is False
    assert utils.is_airtable_record_id("rec1234567890123") is False
    assert utils.is_airtable_record_id("REC12345678901234") is False
    assert utils.is_airtable_record_id(99912345678901234) is False
