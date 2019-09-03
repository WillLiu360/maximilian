import sys, os
import json
import argparse
from pathlib import Path
from cocore.config import Config
from cocore.esLogger import Logger
from cocore.batch import Batch
from codb.pg_tools import PGInteraction

# sys.excepthook = l.handle_exception


class ScriptRunner():
    """
    generic class for execution of a parameterized script in postgres or redshift
    """
    def __init__(self, database):
        conf = Config()
        pg_db_name = conf[database]['db_name']
        pg_user = conf[database]['user']
        pg_host = conf[database]['host']
        pg_password = conf[database]['password']
        pg_port = conf[database]['port']
        self.pg = PGInteraction(dbname=pg_db_name,host=pg_host,user=pg_user,password=pg_password,port=pg_port,schema='public')
        self.pg.conn()

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
            sql = sql.replace(var, val)
        return sql

    def run_script(self, script, from_date= None, to_date=None, batch_id=None, params=None, batchy_job = None):
        """
        method for expanding and running sql statements
        :param script:
        :param from_date:
        :param to_date:
        :param batch_id:
        :param params:
        :return:
        """

        paramset = {}

        # set up logger
        script_path, script_filename = os.path.split(script)
        logger = Logger(job_name=script_filename)
        sys.excepthook = logger.handle_exception

        # Check if file exists
        file = Path(f"{script_path}/{script_filename}").is_file()
        if not file:
            e = 'File not found, please check path'
            logger.l(e)
            raise RuntimeError(e)

        # first we retrive params  we will load these into dict first, any additional params specified will override
        if batchy_job:
            wf = batchy_job.split('.')[0]
            try:
                job = batchy_job.split('.')[1] if len(batchy_job.split('.')[1]) > 0 else 'global'
            except:
                job = 'global'
            batchy_params = Batch(wf).get_status()
            paramset.update(batchy_params[job])

        # next we apply custom params and special metadata fields, again this will overrite batchy params if specified
        # convert string params to dict
        try:
            params = dict((k.strip(), v.strip()) for k, v in (item.split('-') for item in params.split(',')))
        except:
            logger.l("issue parsing params")

        if type(params) == dict:
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

        raw_sql = open(script).read()
        sql = self.expand_params(raw_sql,paramset)
        sql_message = '\n\n--sql script start:\n' + sql + '\n--sql script end\n\n'
        logger.l(sql_message,10)

        self.pg.batchOpen()

        logger.l("starting script")
        try:
            self.pg.exec_sql(sql)
            self.pg.batchCommit()
            logger.l("batch commit")
        except Exception as e:
            logger.l("execution failed with error: " + str(e))
            raise RuntimeError(e)
            # os._exit(1) #this truncates logout in rundeck



if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--script', help="""enter a path to your script """)
    parser.add_argument('-p', '--parameters',  # type=json.loads, default='{"none":"none"}',
                        default='none-none',
                        help="""additional params to be substituted in script, example: -p param1-val1, param2-val2 """)
    parser.add_argument('-d', '--database', help="""db alias from etl.cfg, default is cosmo """, default='cosmo')
    parser.add_argument('-f', '--from_date', help ="""from_date""", default=None)
    parser.add_argument('-t', '--to_date', help="""to_date""", default=None)
    parser.add_argument('-b', '--batch_id', help="""enter batch id """, default=None)
    parser.add_argument('-wf', '--batchy_job', help="""fully qualified batchy job name of the format wf.job""", default=None)
    args = parser.parse_args()

    ScriptRunner(args.database).run_script(args.script, args.from_date, args.to_date, args.batch_id, args.parameters, args.batchy_job)
