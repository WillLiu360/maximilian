#!/usr/bin/env python

import sys, os
import argparse
from datetime import datetime

from cocore.config import Config
from cocore.esLogger import Logger
from codb.mssql_tools import MSSQLInteraction

class MSSQLRunner():
    """
    generic class for execution of a parameterized script in postgres or redshift
    """
    def __init__(self, database):
        conf = Config()
        ms_db_name = conf[database]['db_name']
        ms_user = conf[database]['user']
        ms_server = conf[database]['server']
        ms_password = conf[database]['password']
        self.ms = MSSQLInteraction(dbname=ms_db_name, host=ms_server, user=ms_user, password=ms_password)
        self.ms.conn()

    @staticmethod
    def expand_params(sql, params):
        """
        substitutes params in sql stagement
        
        :param sql:
        :param params:
        :return: sql, expanded with params
        """
        for p in params.keys():
            var = '$[?' + p + ']'
            val = str(params[p])
            if p == 'from_date' and val != '1776-07-04':
                try:
                    val = datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%f')
                    val = val.strftime("%Y-%m-%d %H:%M:%S")
                    print('removed microsecond from wf from_date')
                except Exception as e:
                    print(e)
                    pass
            sql = sql.replace(var, val)
        return sql

    def run_script(self, script, from_date= None, to_date=None, batch_id=None, params=None, sql_command = None):
        """
        method for expanding and running sql statements

        :param script:
        :param from_date:
        :param to_date:
        :param batch_id:
        :param params:
        :param sql_command:
        :return:
        """

        paramset = {}

        # set up logger
        if script is not None:
            script_path, script_filename = os.path.split(script)
            LOG = Logger(job_name=script_filename)
        else:
            LOG = Logger(job_name='direct_sql')
            sys.excepthook = LOG.handle_exception

        # next we apply custom params and special metadata fields
        # convert string params to dict
        try:
            params = dict((k.strip(), v.strip()) for k, v in (item.split('-') for item in params.split(',')))
        except Exception as e:
            LOG.l("issue parsing params: " + str(e))

        if isinstance(params, dict):
            paramset.update(params)

        if from_date:
            paramset['from_date'] = from_date
        if to_date:
            paramset['to_date'] = to_date
        if batch_id:
            paramset['batch_id'] = batch_id

        # now defaults for special metadata fields
        if paramset.get('from_date') is None:
            paramset['from_date'] = '1776-07-04'
        if paramset.get('to_date') is None:
            paramset['to_date'] = '9999-12-31'
        if paramset.get('batch_id') is None:
            paramset['batch_id'] = '-1'
        # we'll keep batch_no for backwards compatibility
        paramset['batch_no'] = paramset['batch_id']

        # check if it's direct SQL or SQL file
        if script is not None:
            raw_sql = open(script).read()
        else:
            raw_sql = sql_command
        sql = self.expand_params(raw_sql, paramset)
        sql_message = '\n\n--sql script start:\n' + sql + '\n--sql script end\n\n'
        LOG.l(sql_message,10)

        self.ms.batchOpen()

        LOG.l("starting script")
        try:
            self.ms.exec_sql(sql)
            self.ms.batch_commit()
            LOG.l("batch commit")
        except Exception as e:
            LOG.l("execution failed with error: " + str(e))
            raise RuntimeError(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--script', help="""enter a path to your script """, default=None)
    parser.add_argument('-p', '--parameters', default='none-none',
                        help="""additional params to be substituted in script, example: -p param1-val1, param2-val2 """)
    parser.add_argument('-d', '--database', help="""db alias from etl.cfg, default is life """, default='life')
    parser.add_argument('-f', '--from_date', help ="""from_date""", default=None)
    parser.add_argument('-t', '--to_date', help="""to_date""", default=None)
    parser.add_argument('-b', '--batch_id', help="""enter batch id """, default=None)
    parser.add_argument('-c', '--sql_command', help="""actual sql command to be executed""", default=None)
    args = parser.parse_args()

    MSSQLRunner(args.database).run_script(args.script, args.from_date, args.to_date, args.batch_id, args.parameters, args.sql_command)
