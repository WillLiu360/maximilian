from unittest.mock import patch, MagicMock
import unittest, sys, json

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
        self.main.stage_to_redshift(
            'dbo.User_UserPreferenceType',
            'edw_landing.stg_webdb_user_userpreferencetype'
        )

        self.assertTrue(True)
