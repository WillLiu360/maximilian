import os
import unittest

from coco.core import config


def get_config_path(file_name):
    this_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(this_dir, 'test_data', file_name)
    return config_path


class TestConfig(unittest.TestCase):
    config_file = 'test_etl.cfg'
    config_path = get_config_path(config_file)
    config_file_search_root = config_path[:-len(config_file)]
    branch_search_root = config_file_search_root[:-len('/test_data')]
    expected_config = {
        "redis-db": {
            "host": "localhost",
            "port": 6379,
            "db": 9,
            "member_db": 9,
            "trainer_db": 10,
            "pt_session_db": 11,
            "barcode_db": 10,
            "password": "test-password-word"
        }
    }

    def test_find_config_path(self):
        result = config.find_config_path(self.config_file, self.config_file_search_root)
        self.assertEqual(self.config_path, result)

    def test_find_config_path_with_branch(self):
        result = config.find_config_path(self.config_file, self.branch_search_root)
        self.assertEqual(self.config_path, result)

    def test__original_config_parser_parameters_load_in_position(self):
        result = config.Config(self.config_path)
        self.assertEqual(self.expected_config, result)

    def test_text_config_parser_parameters_load_in_position(self):
        parser = config.TextConfig(1, 2, 3)
        result = parser.parse_config(self.config_path)
        self.assertEqual(self.expected_config, result)


if __name__ == '__main__':
    unittest.main()
