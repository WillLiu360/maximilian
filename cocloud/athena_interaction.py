import gevent.monkey
gevent.monkey.patch_all()

import boto3
from time import sleep


class AthenaInteraction:
    def __init__(self, aws_access_key, aws_secret_key, region=None):
        try:
            self.client = boto3.client('athena',
                                       region_name=region,
                                       aws_access_key_id=aws_access_key,
                                       aws_secret_access_key=aws_secret_key
                                       )
            print("Connected to Athena client")
        except Exception as e:
            raise e

    def store_query(self, name, description, db, sql):
        """
        Allows a sql query to be saved to Athena for a given database
        Will fail if a query of the same name already exists in the provided database
        """
        existing_queries = self.list_queries()
        for query in existing_queries['NamedQueries']:
            if query['Name'] == name and query['Database'] == db:
                return 'This query already exists in this database'

        response = self.client.create_named_query(
            Name=name,
            Description=description,
            Database=db,
            QueryString=sql
        )

        return response

    def list_queries(self):
        """
        Lists all stored athena queries
        """
        named_query_list = (self.client.list_named_queries())['NamedQueryIds']
        list_of_queries = self.client.batch_get_named_query(
            NamedQueryIds=named_query_list)

        return list_of_queries

    def search_queries_by_db_name(self, db_name):
        """
        Searches for stored queries for a given database
        Returns a list of results
        """
        list_of_queries = self.list_queries()
        results = []
        for query in list_of_queries['NamedQueries']:
            if query['Database'] == db_name:
                results.append(query)

        return results

    def search_queries_by_name(self, name):
        """
        Searches for stored queries by their string name
        Returns a single result
        """
        list_of_queries = self.list_queries()
        for query in list_of_queries['NamedQueries']:
            if query['Name'] == name:
                return query

        raise Exception('Query not found')

    def repair_table(self, db_name, table, output_location=None,
                     partitions=None, s3_data=None):
        """
        Will try and load all partitions if none are specified
        WARNING: Loading all partitions can be subject to long wait times
        """
        try:
            if partitions is None:
                sql = 'MSCK REPAIR TABLE {}'.format(table)
            else:
                if type(partitions) is not dict and type(partitions) is not list:
                    raise Exception('Partitions must be passed as a dict or list')
                if type(partitions) is list:
                    part_str = ''
                    for d in partitions:
                        each_partition = ','.join([i + '=\'' + d[i] + '\'' for i in d])
                        part_str += ' partition ({})'.format(each_partition)
                    sql = """ALTER TABLE {} add{}""".format(table, part_str)
                elif type(partitions) is dict:
                    part_str = ','.join([i + '=\'' + partitions[i] + '\'' for i in partitions])
                    sql = """ALTER TABLE {} add partition ({})""".format(table, part_str)
                if s3_data is not None:
                    sql += """ location '{}'""".format(s3_data)
            results = self.exec_query(sql, db_name, output_location)

            return results
        except Exception as e:
            raise e

    def run_existing_query(self, query_name, output_location=None,
                           optional_vars=[]):
        """
        Will find query by its string name and return the sql and db name
        Optional vars allows dynamic variables to be passed to a saved query
        Optional vars can be a list or tuple. Be sure to escape quotes for strings
        """
        try:
            response = self.search_queries_by_name(query_name)
            sql, db = response['QueryString'].format(
                *optional_vars), response['Database']

            print(sql, db, output_location)
            results = self.exec_query(sql, db, output_location)

            return results

        except Exception as e:
            raise e

    def poll_for_results(self, query_execution_id, output_location):
        """
        Initiate polling to ease network requests
        If query succeeds break and return results otherwise gracefully fail
        Poll every 3 seconds for 120 seconds
        """
        print('polling for results')
        poll_interval = 3
        max_poll_time = 600

        while True:
            execution_details = self.client.get_query_execution(
                QueryExecutionId=query_execution_id)

            if execution_details['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                break
            if execution_details['QueryExecution']['Status']['State'] == 'FAILED':
                raise Exception(
                    execution_details['QueryExecution']['Status']['StateChangeReason'])
            if max_poll_time == 0:
                return 'Query took too long. Query S3 at {} to fetch the results or \
                    query with the following QueryExecutionId {}'.format(output_location, query_execution_id)

            sleep(poll_interval)
            print('Poll time left: ', max_poll_time)
            max_poll_time -= poll_interval
        results = self.client.get_query_results(QueryExecutionId=query_execution_id)

        return results, execution_details

    def exec_query(self, query, db_name, output_location=None,
                   encryption='SSE_S3', kms_key=''):
        """
        Executes an athena query
        Can be called directly or via run_existing_query()
        Encryption options are: SSE_S3 | SSE_KMS | CSE_KMS
        """
        if output_location is None:
            output_location = 's3://aws-athena-query-results-813561490937-us-east-1/python/'
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': db_name},
            ResultConfiguration={
                'OutputLocation': output_location,
                'EncryptionConfiguration': {'EncryptionOption': encryption, 'KmsKey': kms_key}}
        )

        # Wait a few seconds before seeing if the results are ready
        sleep(3)

        # Check if the query is complete
        execution_details = self.client.get_query_execution(
            QueryExecutionId=response['QueryExecutionId'])

        # If the results are not ready then being polling process
        if execution_details['QueryExecution']['Status']['State'] == 'SUCCEEDED':
            results = self.client.get_query_results(
                QueryExecutionId=response['QueryExecutionId'])
        else:
            results, execution_details = self.poll_for_results(
                response['QueryExecutionId'], output_location)

        query_id = response['QueryExecutionId']
        print('Data Scanned (KB): ', execution_details['QueryExecution']['Statistics']['DataScannedInBytes'] / 1000.0)
        print('Seconds Spent: ',
              execution_details['QueryExecution']['Statistics']['EngineExecutionTimeInMillis'] / 1000.0)

        return query_id, results

    def format_results(self, results, delimiter):
        data_table = ''
        for index, row in enumerate(results['ResultSet']['Rows']):
            temp_row = []
            for values in row['Data']:
                if 'VarCharValue' in values:
                    temp_row.append('"' + values['VarCharValue'] + '"')
                else:
                    temp_row.append('""')
            data_table += delimiter.join(temp_row) + '\n'

        return data_table

    def results_formatter(self, results):
        formatted_results = []
        for index, row in enumerate(results['ResultSet']['Rows']):
            if index == 0:  # skip first row with headers
                continue
            values = [value['VarCharValue'] for value in row['Data']]
            if len(values) == 1:
                formatted_results.append(values[0])  # keep as string
            else:    
                formatted_results.append((*values,))  # turn list into tuple

        return formatted_results
