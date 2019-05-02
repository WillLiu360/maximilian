#!/usr/bin/env python
"""
this module provides basic interaction with aws emr service
"""
import gevent.monkey
gevent.monkey.patch_all()

from pprint import pprint
from time import sleep
import boto3
from cocore.config import Config


class EMRCluster:
    """
    wrapper on boto3 emr
    """

    def __init__(self, config_settings='general'):
        """

        :return:
        """
        conf = Config()[config_settings]

        aws_access_key = conf['aws_access_key']
        aws_secret_key = conf['aws_secret_key']
        self.conn = boto3.client('emr',
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key,
                                 region_name='us-east-1')
        self.temp_bucket = 's3://' + conf['temp_bucket'] + '/temp/emr/'
        self.env = conf['env']

    def create_cluster(self, cluster_name='emr-cluster',
                       instance_count=3,
                       instance_type='m3.xlarge',
                       async_mode=True,
                       applications=True,
                       autoTerminate=False,
                       bootstrap_file=None,
                       release_label="emr-5.0.0",
                       log_uri=None):
        """

        :param cluster_name: emr-cluster
        :param instance_count: default 3 (1 master, 2 core)
        :param instance_type: default m3.xlarge
        :param async_mode: default True, True will return right away, False polls until completion
        :return:
        """
        if applications:
            app_list = [{'Name': 'Hadoop'}, {'Name': 'Hive'}, {'Name': 'Hue'}, {'Name': 'Spark'},
                        {'Name': 'HCatalog'}, {'Name': 'Presto'}, {'Name': 'Mahout'}]
        else:
            app_list = []

        if autoTerminate:
            KeepJobFlowAliveWhenNoSteps = False
        else:
            KeepJobFlowAliveWhenNoSteps = True

        # Only supports a single setup file for now
        if bootstrap_file:
            bootstrap_action = [{
                'Name': 'setup',
                'ScriptBootstrapAction': {'Path': bootstrap_file}
            }]
        else:
            bootstrap_action = []

        if not log_uri:
            log_uri = self.temp_bucket

        response = self.conn.run_job_flow(
            Name='-'.join([self.env, cluster_name]),
            LogUri=log_uri,
            ReleaseLabel=release_label,
            VisibleToAllUsers=True,
            Instances={
                'MasterInstanceType': instance_type,
                'SlaveInstanceType': instance_type,
                'InstanceCount': instance_count,
                'Ec2KeyName': 'DataTeam-EMR',
                'KeepJobFlowAliveWhenNoSteps': KeepJobFlowAliveWhenNoSteps,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-2a71665e',
                'EmrManagedMasterSecurityGroup': 'sg-78e8ae03',
                'EmrManagedSlaveSecurityGroup': 'sg-7ee8ae05',
                'ServiceAccessSecurityGroup': 'sg-7de8ae06',
                'AdditionalMasterSecurityGroups': [
                    'sg-a0b4d7c7',
                ],
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Applications=app_list,
            BootstrapActions=bootstrap_action
        )

        cluster_id = response['JobFlowId']
        status = 'STARTING'

        if async_mode:
            pass
        else:
            while status in ('STARTING', 'BOOTSTRAPPING'):
                status = self.conn.describe_cluster(
                    ClusterId=cluster_id)['Cluster']['Status']['State']
                print(status)
                sleep(10)

        sleep(30)  # give the cluster some time to wake up :)

        return cluster_id, status, response

    def list_clusters(self, cluster_states=None, from_date=None, to_date=None):
        """

        :return: clusters['Clusters']
                Keys per cluster
                ID: [{cluster["Id"]}],
                NAME: [{cluster["Name"]}],
                STATUS: [{cluster["Status"]}],
                NormalizedInstanceHours: [{cluster["NormalizedInstanceHours"]}]
        """
        if cluster_states is None:
            cluster_states = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']

        if from_date is None and to_date is None:
            clusters = self.conn.list_clusters(
                ClusterStates=cluster_states)
        elif from_date is None and to_date is not None:
            clusters = self.conn.list_clusters(
                CreatedBefore=to_date,
                ClusterStates=cluster_states)
        elif to_date is None and from_date is not None :
            clusters = self.conn.list_clusters(
                CreatedAfter=from_date,
                ClusterStates=cluster_states)
        else:
            clusters = self.conn.list_clusters(
                CreatedAfter=from_date,
                CreatedBefore=to_date,
                ClusterStates=cluster_states)

        return clusters

    def describe_cluster(self, cluster_id):
        """

        :return:
        """
        return self.conn.describe_cluster(
                    ClusterId=cluster_id)

    def step_script_submit(self, cluster_id, script_path, async_mode=True, args=[]):
        args_str = ' '.join(args)

        response = self.conn.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': script_path.split('/')[-1],
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 's3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',
                        'Args': (
                                '%s %s' % (script_path, str(args_str))
                        ).split(),
                    }
                },
            ]
        )
        status = 'RUNNING'
        step_id = response['StepIds'][0]

        if async_mode:
            pass
        else:
            while status in ('PENDING', 'RUNNING'):
                status, step_response = self.get_step_status(cluster_id, step_id)
                print(status)
                sleep(10)
        return step_id, status, response

    def step_spark_submit(self, cluster_id, script_path="/home/hadoop/run.py",
                          num_executors='2', executor_memory='4G', async_mode=True,
                          args=[]):
        """
        :param cluster_id:
        :param script_path: this will be either local on master node or s3://path
        :param num_executors: default 2
        :param executor_memory: default 2G
        :param async_mode: default True, True will return right away, False polls until completion
        :param args: a list object containing arbitrary arguments to spark script
        :return:
        """

        args_str = ' '.join(args)

        response = self.conn.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': script_path.split('/')[-1],
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': (
                                'spark-submit --deploy-mode cluster '
                                '--master yarn-cluster '
                                '--num-executors %s '
                                '--executor-memory %s '
                                '%s '
                                '%s' % (str(num_executors),
                                        executor_memory, script_path, str(args_str))).split(),
                    }
                },
            ]
        )
        print(response)

        status = 'RUNNING'
        step_id = response['StepIds'][0]

        if async_mode:
            pass
        else:
            while status in ('PENDING', 'RUNNING'):
                status, step_response = self.get_step_status(cluster_id, step_id)
                print(status)
                sleep(10)
        print(step_response)
        return step_id, status, response

    def get_step_status(self, cluster_id, step_id):
        """
        :param cluster_id:
        :param step_id:
        :return:
        """
        response = self.conn.describe_step(ClusterId=cluster_id, StepId=step_id)
        status = response['Step']['Status']['State']
        return status, response

    def kill_cluster(self, cluster_id):
        """

        :param cluster_id:
        :return:
        """
        response = self.conn.terminate_job_flows(
            JobFlowIds=[cluster_id, ])
        return response

    def kill_all_clusters(self):
        """

        :return:
        """
        cluster_list = EMRCluster().list_clusters()['Clusters']
        for cluster in cluster_list:
            self.kill_cluster(cluster['Id'])

    def create_run_kill(self, script_path, cluster_name=None, instance_count=2,
                        num_executors=2, executor_memory='4G', bootstrap_file=None, release_label="emr-5.0.0", args=[]):
        """
        a super simple method for creating a cluster, running a job and killing it
        runs syncronously of course
        :param script_path:
        :param args:
        :return:
        """
        if cluster_name is None:
            cluster_name = script_path[script_path.rfind('/') + 1:]
        print('-------creating cluster')
        cluster_id, cluster_status, cluster_response = \
            self.create_cluster(cluster_name, async_mode=False, instance_count=instance_count, bootstrap_file=bootstrap_file,
                                release_label=release_label)
        print('-------running script')
        step_id, status, response = self.step_spark_submit(cluster_id=cluster_id,
                                                           script_path=script_path,
                                                           args=args,
                                                           async_mode=False,
                                                           num_executors=num_executors,
                                                           executor_memory=executor_memory)

        if status == 'COMPLETED':
            print('-------killing cluster')
            self.kill_cluster(cluster_id)
        # else we leave cluster running

        return cluster_id, status, response

# some test cases
# EMRCluster().kill_all_clusters()
# pprint(EMRCluster().create_cluster('elliott-dev',async=False))
# pprint(EMRCluster().list_clusters())
# pprint(EMRCluster().c.describe_cluster(ClusterId='j-21M4V4C4NGJFQ'))
# pprint (EMRCluster().step_spark_submit('j-21M4V4C4NGJFQ',
#                                        's3://eqxdl/temp/emr/scripts/s2s_omniture_blink_web.py'))
# pprint(EMRCluster().get_step_status('j-36CJ6NR2H9QY7', 's-Y1QGS1WW1XOZ'))
# pprint(EMRCluster().create_run_kill(script_path='s3://eqxdl/dev/support_files/mp_checkins/mp_checkins.py',
#                                     args=['3','hello']))
# pprint(EMRCluster().create_run_kill(script_path='s3://eqxdl/dev/support_files/mp_checkins/mp_checkins.py',
#                                     args=['3','hello'], num_executors= 2, executor_memory='4G'))