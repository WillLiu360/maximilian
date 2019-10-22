import unittest
from unittest.mock import MagicMock

from script_runner.module import ScriptRunner

class TestMain(unittest.TestCase):

    def setUp(self):
        self.main = ScriptRunner('cosmo')

    def test_expand_params(self):
        """
        Test parameter substitution function
        :return:
        """
        self.main.pg = MagicMock()
        try:
            sql = "SELECT field1, field2 FROM test WHERE field1 = '$[?var1]' AND field2 = '$[?var2]'"
            params = {
                'var1': 'result1',
                'var2': 'result2'
            }
            result = self.main.expand_params(
                sql,
                params
            )
            expected_result = "SELECT field1, field2 FROM test WHERE field1 = 'result1' AND field2 = 'result2'"
            self.assertEqual(result, expected_result);
        except Exception as e:
            self.assertTrue(False, e)


if __name__ == '__main__':
    unittest.main()
