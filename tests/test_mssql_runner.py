import unittest
from unittest.mock import MagicMock

from mssql_runner.module import MSSQLRunner
from datetime import datetime


class TestMain(unittest.TestCase):

    def setUp(self):
        pass

    def test_expand_params(self):
        """
        Test parameter substitution function
        :return:
        """
        self.main = MSSQLRunner('life')
        try:
            sql = "SELECT field1, field2 FROM test WHERE field1 = '$[?var1]' AND field2 = '$[?var2]'"
            params = {
                'var1': 'result1',
                'var2': 'result2',
                'from_date': '2019-10-23T15:55:41.498998'
            }
            result = self.main.expand_params(
                sql,
                params
            )
            expected_result = "SELECT field1, field2 FROM test WHERE field1 = 'result1' AND field2 = 'result2'"
            self.assertEqual(result, expected_result)

        except Exception as e:
            self.assertTrue(False, e)

    def test_expand_params_error(self):
        self.main = MSSQLRunner('life')
        """
        Test parameter substitution function
        :return:
        """
        try:
            sql = "SELECT field1, field2 FROM test WHERE field1 = '$[?var1]' AND field2 = '$[?var2]'"
            params = {
                'var1': 'result1',
                'var2': 'result2',
                'from_date': '2019-10-23T07:53:51Z'
            }
            result = self.main.expand_params(
                sql,
                params
            )
            expected_result = "SELECT field1, field2 FROM test WHERE field1 = 'result1' AND field2 = 'result2'"
            self.assertEqual(result, expected_result)

        except Exception as e:
            self.assertTrue(False, e)

    def test_run_script(self):
        self.main = MSSQLRunner('life')
        self.main.ms = MagicMock()
        self.main.run_script(
                            script=None,
                            from_date=datetime.now(),
                            to_date=datetime.now(),
                            batch_id=1,
                            params="none-none",
                            sql_command='select * from user'
        )


if __name__ == '__main__':
    unittest.main()
