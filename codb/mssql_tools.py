"""
simple class for interacting with MSSQL
"""
import csv
import re

import pytds
from cocore.Logger import Logger
from cocore.config import Config

LOG = Logger()
CONF = Config()


def _resultIter(cursor, arraysize):
    'An iterator that uses fetchmany to keep memory usage down'
    count = 0
    while True:
        results = cursor.fetchmany(arraysize)
        count += len(results)
        if not results:
            LOG.l('no rows to process')
            break
        LOG.l('%s rows processed'%count)
        for result in results:
            yield result


NON_CSV_CHARS = re.compile(r"[\t\n]")


def csv_cleanup(raw_s):
    encoded = str(raw_s).encode("utf-8")
    return NON_CSV_CHARS.sub(' ', encoded.decode())


class MSSQLInteraction:
    """

    """
    def __init__(self, dbname=None, host=None, user=None, password=None, connection=None, port=1433):
        """

        :param dbname:
        :param host:
        :param user:
        :param password:
        :param connection:
        :return:
        """

        # if there is a connection, we will pull from config,
        # else we use the individual connection pararameters (this is really just for backward compatibility
        if connection:
            user = CONF[connection]['user']
            password = CONF[connection]['password']
            host = CONF[connection]['server']
            dbname = CONF[connection]['db_name']
            try:
                port = CONF[connection]['port']
            except KeyError:
                port = 1433

        else:
            if not dbname or not host or not user or password is None:
                raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname
        self.port = port
        self.con = None

    def conn(self, dict_cursor=False):
        """Open a connection, should be done right before time of insert
        """
        self.con = pytds.connect(self.host, self.dbname, self.user, self.password, port=self.port, as_dict=dict_cursor, login_timeout=360)

    def batchOpen(self):
        """
        :return:
        """
        self.cur = self.con.cursor()

    def _execute_with_or_without_params(self, sql, params):
        """
        :param sql: The query you want run, which may require parameters
        :param params: The query parameters you should escape, may be None
        :return: None
        """
        if params:
            if not isinstance(params, tuple):
                raise ValueError(
                    "Passed in parameters must be in a tuple: %s",
                    params)
            self.cur.execute(sql, params)
        else:
            self.cur.execute(sql)

    def fetch_sql_all(self, sql, params=None):
        """
        :param sql:
        :param params:
        :return:
        """
        try:
            self._execute_with_or_without_params(sql, params)
            results = self.cur.fetchall()
        except Exception as e:
            raise
        return results

    def fetch_sql(self, sql, blocksize=1000, params=None):
        """
        :param sql:
        :param params:
        :return:
        """
        try:
            self._execute_with_or_without_params(sql, params)
            results = _resultIter(self.cur, arraysize=blocksize)
        except Exception as e:
            raise
        return results

    def fetch_sql_one(self, sql):
        """
        :param sql:
        :return:
        """
        statements = sql.strip().split(';')
        statements = list(filter(lambda x: x.strip() != '', statements))
        index = 0
        for statement in statements:
            if index != len(statements) - 1:
                self.exec_sql(statement)
            else:  # Assuming last statement is a select query.
                results = self.fetch_sql_all(statement)
                for row in results:
                    return row

            index += 1

    def export_sql_to_csv(self,
                          sql,
                          csv_filename,
                          delimiter=',',
                          headers=True,
                          params=None):
        """
        :param sql:
        :param csv_filename:
        :param delimiter:
        :return:
        """
        result = self.fetch_sql(sql, params)

        f = open(csv_filename, 'w', newline='')
        LOG.l('exporting to file:' + f.name)
        writer = csv.writer(f, delimiter=delimiter)
        if headers:
            # write headers if we have them
            writer.writerow([i[0] for i in self.cur.description])
        for row in result:
            writer.writerow([csv_cleanup(s) for s in row])
        f.flush()
        f.close()

    def getTableColumns(self, table_name):
        """
        :param table_name:
        :return:
        """
        name_parts = table_name.split('.')
        schema = name_parts[0]
        table = name_parts[1]
        sql = """
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = %s and table_name = %s;"""
        return self.fetch_sql_all(sql, params=(schema, table))

    def exec_sql(self, sql, auto_commit=True):
        """

        :param sql:
        :param auto_commit:
        :return:
        """
        self.cur.execute(sql)
        if auto_commit:
            self.con.commit()

    def batch_commit(self):
        """

        :return:
        """
        self.con.commit()

    def batchClose(self):
        """

        :return:
        """
        self.con.close()
