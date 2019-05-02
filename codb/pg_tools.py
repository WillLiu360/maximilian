import csv
import psycopg2
import psycopg2.extras
import simplejson as json

from psycopg2 import errorcodes
from cocore.Logger import Logger

l = Logger()


def _resultIter(cursor, arraysize):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


class PGInteraction:
    """
    """

    def __init__(self, dbname, host, user, password, port, schema="public"):
        """
        """
        if not dbname or not host or not user or password is None:
            raise RuntimeError("%s request all __init__ arguments" % __name__)

        self.host = host
        self.user = user
        self.password = password
        self.dbname = dbname
        self.port = port

    def conn(self, dict_cursor=False):
        """Open a connection, should be done right before time of insert
        """
        self.con = psycopg2.connect("dbname=" + self.dbname + " host=" + self.host +
                                    " user=" + self.user + " password=" + self.password + " port=" + str(self.port)
                                    )
        self.dict_cursor = dict_cursor

    def batchOpen(self):
        """
        :return:
        """
        if self.dict_cursor:
            self.cur = self.con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        else:
            self.cur = self.con.cursor()

    def batchCommit(self):
        """
        """
        try:
            self.con.commit()
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
            # Logger().l("pgError")

    def fetch_sql_all(self, sql):
        """
        :param sql:
        :return:
        """
        try:
            self.cur.execute(sql)
            results = self.cur.fetchall()
        except Exception as e:
            raise
        return results

    def fetch_sql(self, sql, blocksize=1000):
        """
        :param sql:
        :return:
        """
        try:
            self.cur.execute(sql)
            results = _resultIter(self.cur, arraysize=blocksize)
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
        return results

    def export_sql_to_csv(self, sql, csv_filename, delimiter=',', headers=True):
        """
        :param sql:
        :param csv_filename:
        :param delimiter:
        :return:
        """
        result = self.fetch_sql(sql)

        f = open(csv_filename, 'w', newline='')
        l.l('exporting to file:' + f.name)
        writer = csv.writer(f, delimiter=delimiter)
        if headers:
            writer.writerow([i[0] for i in self.cur.description])  # write headers
        for row in result:
            writer.writerow([str(s).replace('\t', ' ').replace('\n', ' ') for s in row])
        f.flush()
        f.close()

    def export_sql_to_json(self, sql, filename):
        results = self.fetch_sql(sql)

        with open(filename, 'w') as f:
            json_results = json.dumps(
                (record for record in results), iterable_as_array=True)
            f.write(json_results)

    def export_sql_to_s3(self, sql, s3path, aws_access_key, aws_secret_key, options=None):
        """
        where option is an array of:
            { MANIFEST
            | DELIMITER [ AS ] 'delimiter-char'
            | FIXEDWIDTH [ AS ] 'fixedwidth-spec' }
            | ENCRYPTED
            | BZIP2
            | GZIP
            | ADDQUOTES
            | NULL [ AS ] 'null-string'
            | ESCAPE
            | ALLOWOVERWRITE
            | PARALLEL [ { ON | TRUE } | { OFF | FALSE } ]
            [ MAXFILESIZE [AS] max-size [ MB | GB ] ]
        """
        default_options = ["delimiter '|'", "ALLOWOVERWRITE", "PARALLEL false"]

        if options is None:
            options = default_options

        self.unload_stmt = """unload ('%s') to '%s' credentials 'aws_access_key_id=%s;aws_secret_access_key=%s' %s""" % (
        sql, s3path, aws_access_key, aws_secret_key, ' '.join(options))
        print("unload command %s " % self.unload_stmt)
        self.exec_sql(self.unload_stmt)
        print("Unload complete")

    def exec_sql(self, sql):
        """
        :param sql:
        :return:
        """
        try:
            results = self.cur.execute(sql)
        except Exception as e:
            l.l(e)
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)
        return results

    def bulkDictionaryInsert(self, table_name, col_dict):
        """
        """
        if len(col_dict) == 0:
            return

        placeholders = ', '.join(['%s'] * len(col_dict))
        columns = ', '.join(col_dict.keys())

        sql = "INSERT into %s ( %s ) VALUES ( %s )" % (table_name, columns, placeholders)

        try:
            self.cur.execute(sql, list(col_dict.values()))
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)

    def bulkPostCleanup(self, table_name):
        """
        """
        sql = """
          delete from {0}
          where etl_updated=0
          and nk in (select nk from {0} where etl_updated = 1);

          update {0} set etl_updated = 0 where etl_updated = 1;""".format(table_name)

        try:
            self.cur.execute(sql)
        except Exception as e:
            pgError = errorcodes.lookup(e.pgcode)
            raise RuntimeError(pgError)

    def table_exists(self, table_name):
        """Checks for the existence of a table in specified redshift environment

        :param connection: A redshift connection
        :param table_name: The table name, including schema, that should be checked for existence
        :return: A boolean value indicating the tables existence
        """
        schema_name = 'public'

        if '.' in table_name:
            schema_name, table = table_name.split('.')
        else:
            table = table_name

        sql = """select
              case when count(1) = 1 then true else false end
              from pg_class c
              join pg_namespace n on n.oid = c.relnamespace
              where trim(n.nspname) = '%s' and trim(c.relname) = '%s'
              """ % (schema_name.lower(), table.lower())

        return self.fetch_sql_all(sql)[0]

    def batchClose(self):
        """

        :return:
        """
        self.con.close()
