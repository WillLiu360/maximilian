import ast
import decimal
import datetime
import json
import os
from string import Template

import redis

from codb.sf_tools import SFInteraction

from codb.pg_tools import PGInteraction
from codb.mssql_tools import MSSQLInteraction
from cocore.Logger import Logger

LOG = Logger()


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):  # handles both date and datetime objects
            return obj.isoformat()
        else:
            if isinstance(obj, decimal.Decimal):
                # return (str(o) for o in [obj])
                return str(obj)
            elif isinstance(obj, bytes):
                return obj.decode("utf-8")
            return json.JSONEncoder.default(self, obj)


class Template(Template):
    idpattern = r'[a-z][_a-z0-9]*(\.[a-z][_a-z0-9]*)*'


class CacheManager(object):
    def __init__(self):
        self.rconn = None
        self.pipeline = None
        self.sqldb = None
        self.sfdc = None
        self.use_json = False

    def setup_cache(self, redis_server, redis_port, redis_db, save_as_json=False):
        self.rconn = redis.StrictRedis(host=redis_server, port=redis_port, db=redis_db)
        self.use_json = save_as_json
        self.pipeline = self.rconn.pipeline()
        return self

    def setup_pg_db(self, server, port, db_name, username, password, schema='public'):
        self.sqldb = PGInteraction(dbname=db_name,
                                   host=server,
                                   user=username,
                                   password=password,
                                   port=port,
                                   schema=schema)

        self.sqldb.conn(dict_cursor=True)
        self.sqldb.batchOpen()
        return self

    def setup_sql_db(self, server, db_name, username, password):
        self.sqldb = MSSQLInteraction(
            dbname=db_name,
            host=server,
            user=username,
            password=password
        )
        self.sqldb.conn(dict_cursor=True)
        self.sqldb.batchOpen()
        return self

    def setup_salesforce(self, username, password, token, uat):
        self.sfdc = SFInteraction(username, password, token, uat)
        return self

    def cache_sql_long(self,
                       sql,
                       key_field,
                       subkey_field,
                       value_field,
                       prefix="",
                       suffix="",
                       use_zset=False,
                       TTL=0):

        rows = self.sqldb.fetch_sql(sql)

        count = 0
        last_seen_key = ""
        data = {}

        for row in rows:
            keyname = prefix + str(row[key_field]).strip() + suffix

            print(("reading data for " + keyname))

            temp_keyname = "t-" + last_seen_key
            if (keyname != last_seen_key) and (last_seen_key != ""):
                print(("writing data to key " + last_seen_key))
                if use_zset:
                    self.pipeline.zadd(temp_keyname, **data)
                else:
                    self.pipeline.hmset(temp_keyname, data)
                if TTL > 0:
                    self.pipeline.expire(temp_keyname, TTL)
                self.pipeline.rename(temp_keyname, last_seen_key)

                data = {}

                count = count + 1
                if count >= 1000:
                    self.pipeline.execute()
                    count = 0

            subkeyname = str(row[subkey_field]).strip()
            value = row[value_field]
            data[subkeyname] = value
            last_seen_key = keyname

        print(("final ... writing data to key " + last_seen_key))
        if use_zset:
            self.pipeline.zadd(temp_keyname, **data)
        else:
            self.pipeline.hmset(temp_keyname, data)
        if TTL > 0:
            self.pipeline.expire(temp_keyname, TTL)

        self.pipeline.rename(temp_keyname, keyname)
        self.pipeline.execute()

        self.sqldb.batchClose()

    def cache_sql(self, sql, key_field, prefix="", suffix="",
                  hash_key=None, timestamp_key=None, version=None,
                  version_key=None, TTL=0, template=None,
                  reload_cache=False):
        rows = self.sqldb.fetch_sql(sql)

        if template:
            rows = self.rename_fields(rows, template)

        count = 0

        prefix = self.define_prefix(reload_cache, prefix)

        # if we're using as hash set AND storing data as JSON, we're probably
        # building a cache that should be updated in-place rather than a trunc-and-load
        # style overwrite
        if self.use_json and hash_key is not None:
            overwrite = False
        else:
            overwrite = True

        for row in rows:
            keyname = ""
            temp_keyname = ""
            data = {}

            for col in row:
                if col == key_field:
                    keyname = prefix + str(row[col]) + suffix
                    if overwrite:
                        temp_keyname = "t-" + keyname
                    else:
                        temp_keyname = keyname
                else:
                    try:
                        if row[col]:
                            if isinstance(row[col], bool):
                                data[col] = row[col]
                            elif isinstance(row[col], str):
                                data[col] = row[col].encode('utf-8').strip()
                            else:
                                data[col] = str(row[col]).strip()
                    except Exception as e:
                        LOG.l('{}: {}'.format(row.encode('utf-8'), e))

            # print keyname
            if self.use_json:
                if hash_key:
                    current_key = self.rconn.hget(str(temp_keyname), hash_key)
                    if current_key:
                        try:
                            current_key = json.loads(current_key)
                        except:
                            current_key = None
                        if current_key:
                            for data_key in list(data.keys()):
                                if data[data_key]:
                                    current_key[data_key] = data[data_key]
                        else:
                            current_key = data
                    else:
                        current_key = data
                    self.pipeline.hset(name=temp_keyname, key=hash_key, value=json.dumps(current_key, cls=JSONEncoder))
                    if timestamp_key:
                        self.pipeline.hset(name=temp_keyname, key=timestamp_key,
                                           value=str(datetime.datetime.now()))
                    if version_key:
                        self.pipeline.hset(name=temp_keyname, key=version_key,
                                           value=str(version))
                else:
                    self.pipeline.set(name=temp_keyname, value=json.dumps(data, cls=JSONEncoder))
            else:
                self.pipeline.hmset(temp_keyname, data)

            if TTL > 0:
                self.pipeline.expire(temp_keyname, TTL)

            if overwrite:
                self.pipeline.rename(temp_keyname, keyname)

            count = count + 1
            if count >= 1000:
                self.pipeline.execute()
                count = 0

        self.pipeline.execute()

        self.sqldb.batchClose()

    def update_cache_from_sql(self, sql, prefix, key_field, TTL=0):
        rows = self.sqldb.fetch_sql(sql)
        count = 0
        for row in rows:
            keyname = ""
            temp_keyname = ""
            data = {}

            for col in row:
                print(f'col: {col}')
                if col == key_field:
                    keyname = prefix + str(row[col])
                    temp_keyname = "t-" + keyname
                else:
                    if row[col] == 0 or row[col]:
                        data[col] = str(row[col])
                    else:
                        data[col] = None

            print(("updating " + str(keyname)))
            if self.use_json:
                try:
                    current_key = self.rconn.get(str(keyname))
                except Exception as e:
                    current_key = None
                if current_key:
                    try:
                        current_key = json.loads(current_key)
                    except:
                        current_key = None
                    if current_key:
                        for data_key in list(data.keys()):
                            if data[data_key] == 0 or data[data_key]:
                                current_key[data_key] = data[data_key]
                            else:
                                if data_key in current_key:
                                    del current_key[data_key]
                    else:
                        current_key = data
                else:
                    current_key = data

                self.pipeline.set(name=temp_keyname, value=json.dumps(current_key, cls=JSONEncoder))
            else:
                self.pipeline.hmset(temp_keyname, data)

            if TTL > 0:
                self.pipeline.expire(temp_keyname, TTL)

            self.pipeline.rename(temp_keyname, keyname)

            count = count + 1
            if count >= 1000:
                self.pipeline.execute()
                count = 0

        self.pipeline.execute()

        self.sqldb.batchClose()

    def rename_fields(self, records, template):
        """For consistent naming convention of hash fields.

        Given a json mapping template and dictionary records,
        returns renamed keys for dict records.
        """
        src = Template(template)

        for r in records:
            try:
                mapped_r = src.substitute(r)
                transformed_r = self.transform_to_json_types(mapped_r)
                yield json.loads(transformed_r)
            except KeyError as e:
                LOG.l('Key error: {} for {}'.format(e, r))
            except Exception as e:
                LOG.l("Error: {} for {}".format(e, r))
                pass

    def transform_to_json_types(self, record):
        record = record.replace('"None"', 'null')
        record = record.replace('"False"', 'false')
        record = record.replace('"True"', 'true')

        return record

    def save_results_to_file(self, filename, records):
        with open(filename, 'w') as f:
            for r in records:
                f.write('\n')
                f.write(str(r))

    def get_results_from_file(self, filename):
        with open(filename, 'r') as f:
            records = f.read()[1:].splitlines()  # skip blank first line
            for r in records:
                r = ast.literal_eval(r)  # convert str to dict
                yield r

    def cache_salesforce(self, soql_query, db_table, key_field,
                         prefix="", suffix="", hash_key=None,
                         timestamp_key=None, version=None,
                         version_key=None, TTL=0, template=None,
                         reload_cache=False):
        salesforce_records = self.sfdc.get_query_records_dict(db_table, soql_query)

        # save to and read from file to avoid connection timeout
        salesforce_pt_file = 'sf_pt_results.txt'
        self.save_results_to_file(salesforce_pt_file, salesforce_records)
        records = self.get_results_from_file(salesforce_pt_file)

        # Use mapping template for rename of keys / hash fields
        if template:
            rows = self.rename_fields(records, template)
        else:
            rows = salesforce_records

        count = 0

        prefix = self.define_prefix(reload_cache, prefix)

        # if we're using as hash set AND storing data as JSON, we're probably
        # building a cache that should be updated in-place rather than a trunc-and-load
        # style overwrite
        if self.use_json and hash_key is not None:
            overwrite = False
        else:
            overwrite = True

        for row in rows:
            keyname = ""
            temp_keyname = ""
            data = {}

            for col in row:
                if col == key_field:
                    keyname = prefix + str(row[col]) + suffix
                    if overwrite:
                        temp_keyname = "t-" + keyname
                    else:
                        temp_keyname = keyname
                else:
                    try:
                        if row[col]:
                            if isinstance(row[col], bool):
                                data[col] = row[col]
                            elif isinstance(row[col], str):
                                data[col] = row[col].encode('utf-8').strip()
                            else:
                                data[col] = str(row[col]).strip()
                    except Exception as e:
                        LOG.l('{}: {}'.format(row.encode('utf-8'), e))


            if self.use_json:
                if hash_key:
                    current_key = self.rconn.hget(str(temp_keyname), hash_key)
                    if current_key:
                        try:
                            current_key = json.loads(current_key)
                        except:
                            current_key = None
                        if current_key:
                            for data_key in list(data.keys()):
                                if data[data_key]:
                                    current_key[data_key] = data[data_key]
                        else:
                            current_key = data
                    else:
                        current_key = data
                    self.pipeline.hset(name=temp_keyname, key=hash_key, value=json.dumps(current_key, cls=JSONEncoder))
                    if timestamp_key:
                        self.pipeline.hset(name=temp_keyname, key=timestamp_key,
                                           value=str(datetime.datetime.now()))
                    if version_key:
                        self.pipeline.hset(name=temp_keyname, key=version_key,
                                           value=str(version))
                else:
                    self.pipeline.set(name=temp_keyname, value=json.dumps(data, cls=JSONEncoder))
            else:
                self.pipeline.hmset(temp_keyname, data)

            if TTL > 0:
                self.pipeline.expire(temp_keyname, TTL)

            if overwrite:
                self.pipeline.rename(temp_keyname, keyname)

            count = count + 1
            if count >= 1000:
                self.pipeline.execute()
                count = 0

        self.pipeline.execute()

        os.remove(salesforce_pt_file)

    def fetch_mssql(self, sql):
        rows = self.sqldb.fetch_sql(sql)

        return rows

    def encode_data(self, row):
        data = {}

        # print row
        for col in row:
            try:
                if row[col]:
                    if isinstance(row[col], bool):
                        data[col] = row[col]
                    elif isinstance(row[col], str):
                        data[col] = row[col].encode('utf-8').strip()
                    else:
                        data[col] = str(row[col]).strip()
            except Exception as e:
                LOG.l('{}: {}'.format(row.encode('utf-8'), e))

        return data

    def append_multiple_records(self, sorted_rows, unique_id):
        last_seen_key = ""
        result = {}
        data = []

        for row in sorted_rows:
            keyname = row[unique_id]
            # print ("reading data for " + keyname)

            if (keyname != last_seen_key) and (last_seen_key != ""):
                result[last_seen_key] = data
                yield result

                data = []
                last_seen_key = ""
                result = {}
            row.pop(unique_id)
            encoded_row = self.encode_data(row)
            data.append(encoded_row)
            last_seen_key = keyname

        result[last_seen_key] = str(data)
        yield result

    def cache_multiple_records_to_field(
            self, data, key_field, prefix="", suffix="",
            hash_key=None, timestamp_key=None,
            version=None, version_key=None, TTL=0,
            reload_cache=False):
        count = 0

        prefix = self.define_prefix(reload_cache, prefix)

        if self.use_json and hash_key is not None:
            overwrite = False
        else:
            overwrite = True

        for row in data:
            keyname = prefix + list(row.keys())[0] + suffix
            temp_keyname = ""

            if overwrite:
                temp_keyname = "t-" + keyname
            else:
                temp_keyname = keyname

            # print keyname
            if self.use_json:
                if hash_key:
                    self.pipeline.hset(name=temp_keyname, key=hash_key,
                                       value=json.dumps(list(row.values())[0], cls=JSONEncoder))
                    if timestamp_key:
                        self.pipeline.hset(name=temp_keyname, key=timestamp_key,
                                           value=str(datetime.datetime.now()))
                    if version_key:
                        self.pipeline.hset(name=temp_keyname, key=version_key,
                                           value=str(version))
                else:
                    self.pipeline.set(name=temp_keyname, value=json.dumps(list(row.values())[0], cls=JSONEncoder))
            else:
                self.pipeline.hmset(temp_keyname, list(row.values())[0])

            if TTL > 0:
                self.pipeline.expire(temp_keyname, TTL)

            if overwrite:
                self.pipeline.rename(temp_keyname, keyname)

            count = count + 1
            if count >= 1000:
                self.pipeline.execute()
                count = 0

        self.pipeline.execute()

        self.sqldb.batchClose()

    def define_prefix(self, reload_status, prefix):
        temp_prefix = 'temp_' + prefix
        prefix = temp_prefix if reload_status else prefix

        return prefix
