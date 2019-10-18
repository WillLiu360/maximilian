from unittest.mock import patch, MagicMock
import unittest, sys, json
from unittest.mock import patch

from rsqoop_runner.module import rSqoop

"""
 You can use this test as reference on how to start using rSqoop in your code.
 truncate and load from mssql to postgres/redshift
"""
class TestMain(unittest.TestCase):

    def setUp(self):
        """
            rSqoop(mssql database, postgres/redshift database)
        """
        self.main = rSqoop('WebDB', 'cosmo')

    def test_initial(self):
        """
            main.stage_to_redshift(mssql table, postgres/redshift table)
        """
        self.main.sql = MagicMock()
        self.main.sql.fetch_sql_all.side_effect = [[('UserPreferenceTypeId', 'int', None, 10, 0), ('PreferenceName', 'varchar', 100, None, None), ('PreferenceValueDataType', 'varchar', 50, None, None), ('UserPreferenceCategoryId', 'int', None, 10, 0), ('IsMaintainHistory', 'bit', None, None, None), ('CreatedOn', 'datetime', None, None, None), ('ModifiedOn', 'datetime', None, None, None), ('CreatedBy', 'varchar', 50, None, None), ('ModifiedBy', 'varchar', 50, None, None), ('IsActive', 'bit', None, None, None)],
                                                    [(3,)]]
        self.main.pg_conn = MagicMock()
        self.main.pg_conn.fetch_sql_all.return_value = [(3,)]

        self.main.s3_conn = MagicMock()
        self.main.conf = {
            'general': {
                'temp_bucket': 'test'
            },
        }
        self.main.s3_env = 'test'

        self.main.stage_to_redshift(
            'dbo.User_UserPreferenceType',
            'edw_landing.stg_webdb_user_userpreferencetype'
        )

        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
