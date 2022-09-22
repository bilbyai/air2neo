import unittest

from air2neo.config import (
    keep_col_rule_default,
    is_edge_rule_default,
    is_prop_rule_default,
    format_edge_col_name_default,
)


class TestConfig(unittest.TestCase):
    def test_keep_col_rule_default(self):
        self.assertEqual(keep_col_rule_default(None), False)
        self.assertEqual(keep_col_rule_default(1234), False)
        self.assertEqual(keep_col_rule_default("_id"), False)
        self.assertEqual(keep_col_rule_default("id"), True)
        self.assertEqual(keep_col_rule_default("_id__"), False)

    def test_is_edge_rule_default(self):
        self.assertEqual(is_edge_rule_default(None), False)
        self.assertEqual(is_edge_rule_default(1234), False)
        self.assertEqual(is_edge_rule_default("CONTAINS"), True)
        self.assertEqual(is_edge_rule_default("contains"), False)
        self.assertEqual(is_edge_rule_default("CONTAINS_ENTITIES"), True)

    def test_is_prop_rule_default(self):
        self.assertEqual(is_prop_rule_default(None), False)
        self.assertEqual(is_prop_rule_default(1234), False)
        self.assertEqual(is_prop_rule_default("CONTAINS"), False)
        self.assertEqual(is_prop_rule_default("contains"), True)
        self.assertEqual(is_prop_rule_default("CONTAINS_ENTITIES"), False)

    def test_format_edge_col_name_default(self):
        self.assertEqual(format_edge_col_name_default("CONTAINS"), "CONTAINS")
        self.assertEqual(format_edge_col_name_default("CONTAINS__COMPANY"), "CONTAINS")
