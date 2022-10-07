import unittest

from air2neo.config import format_edge_col_name_default


class TestConfig(unittest.TestCase):
    def test_format_edge_col_name_default(self):
        self.assertEqual(format_edge_col_name_default("CONTAINS"), "CONTAINS")
        self.assertEqual(format_edge_col_name_default("CONTAINS__COMPANY"), "CONTAINS")
