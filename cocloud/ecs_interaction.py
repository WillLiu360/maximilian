#!/usr/bin/env python
"""
this module provides basic interaction with aws ecs service
"""
import gevent.monkey
gevent.monkey.patch_all()

from pprint import pprint
from time import sleep
import boto3
from cocore.config import Config


class ECSInteraction:
    """
    wrapper on boto3 ecs
    """
    def __init__(self, config_settings='ECS'):
        """

        :return:
        """
        conf = Config()[config_settings]

        aws_access_key = conf['aws_access_key']
        aws_secret_key = conf['aws_secret_key']
        self.conn = boto3.client('ecs',
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key,
                                 region_name='us-east-1')

    def wait_task(self, cluster, tasks):
        """Wait for task to finish"""
        waiter = self.conn.get_waiter('tasks_stopped')
        response = waiter.wait(cluster=cluster, tasks=tasks,
                               WaiterConfig={'Delay': 10, 'MaxAttempts': 720})

    def get_task_definition(self, task):
        resp = self.conn.describe_task_definition(taskDefinition=task)
        return resp

    def actual_describe_task(self, cluster, tasks):
        return self.conn.describe_tasks(cluster=cluster, tasks=tasks)

    def stop_task(self, cluster, task, reason):
        return self.conn.stop_task(cluster=cluster, task=task, reason=reason)

    def get_task_status(self, cluster=None, tasks=None):
        """Return ARN of stopped task"""

        response = self.conn.describe_tasks(
            cluster=cluster, tasks=tasks)

        if not response.get('tasks'):
            raise RuntimeError('Could not find completed task')

        if response['failures'] != []:
            raise Exception('There were failures:\n{0}'.format(
            response['failures']))

        statuses = [t['lastStatus'] for t in response['tasks']]

        if not all([status == 'STOPPED' for status in statuses]):
            raise Exception('Not all tasks finished! :(')

        exit_codes = [c['exitCode'] for t in response['tasks'] for c in t['containers']]

        if all([exit_code == 0 for exit_code in exit_codes]):  # all goes well
            return True
        else:
            # only get exit reasons for unsuccessful tasks
            print('Unsuccessful tasks exited with reason(s): ' + \
                ", ".join([t['stoppedReason'] for t in response['tasks'] for c in t['containers'] if c['exitCode'] != 0]))
            return False

    @staticmethod
    def get_logs_info(defaults):
        """Get the logs prefix and group names"""

        log_config = defaults['taskDefinition']['containerDefinitions'][0]['logConfiguration']

        logs_stream_prefix = log_config['options']['awslogs-stream-prefix']
        logs_group = log_config['options']['awslogs-group']

        return logs_stream_prefix, logs_group

    def list_tasks(self, cluster, desiredStatus):
        response = self.conn.list_tasks(
            cluster=cluster,
            desiredStatus=desiredStatus,
        )
        return response

    def run_task(self, cluster, task_definition, launch_type='EC2', command=None, count=1,
                 environment=None, cpu=None, memory=None, memory_reservation=None,
                 subnets=None, security_groups=None, assign_public_ip=None):
        """Run an ECS task on a given cluster with optional command overrides"""

        defaults = self.get_task_definition(task_definition)

        logs_stream_prefix, logs_group = self.get_logs_info(defaults) # assumes uses aws logs?

        container_defaults = defaults['taskDefinition']['containerDefinitions'][0]

        overrides = {}
        container_overrides = {}

        container_overrides['name'] = task_definition  # silly to overide

        if command:
            print('Using custom command: {}'.format(command))
            container_overrides['command'] = command
        else:
            if 'command' in container_defaults:
                container_overrides['command'] = container_defaults['command']

        if environment:
            print('Using custom env variables: {}'.format(environment))
            container_overrides['environment'] = environment
        else:
            if 'environment' in container_defaults:
                container_overrides['environment'] = container_defaults['environment']

        if cpu:
            print('Using custom cpu units: {}'.format(cpu))
            container_overrides['cpu'] = cpu
        else:
            if 'cpu' in container_defaults:
                container_overrides['cpu'] = container_defaults['cpu']

        if memory:
            print('Using custom hard memory limit: {}'.format(memory))
            container_overrides['memory'] = memory
        else:
            if 'memory' in container_defaults:
                container_overrides['memory'] = container_defaults['memory']
            else:
                container_overrides['memory'] = 128

        if memory_reservation:
            print('Using custom soft memory limit: {}'.format(memory_reservation))
            container_overrides['memoryReservation'] = memory_reservation
        else:
            if 'memoryReservation' in container_defaults:
                container_overrides['memoryReservation'] = container_defaults['memoryReservation']
            else:
                container_overrides['memoryReservation'] = 512

        container_overrides_list = []
        container_overrides_list.append(container_overrides)

        overrides['containerOverrides'] = container_overrides_list

        print('\noverrides: {}'.format(overrides))

        if launch_type == 'FARGATE':  # need to set network settings
            network_config = {
                'awsvpcConfiguration': {
                    'assignPublicIp': assign_public_ip,
                    'subnets': subnets.split(','),
                    'securityGroups': security_groups.split(',')
                }

            }

            print(f'Launch type is FARGATE so using network config: {network_config}')

            response = self.conn.run_task(
                cluster=cluster,
                taskDefinition=task_definition,
                overrides=overrides,
                launchType=launch_type,
                count=count,
                networkConfiguration=network_config)

        else: # regular ec2

            response = self.conn.run_task(
                cluster=cluster,
                taskDefinition=task_definition,
                overrides=overrides,
                launchType=launch_type,
                count=count
            )

        if response['failures']:
            raise Exception(", ".join(["fail to run task {0} reason: {1}".format(
                failure['arn'], failure['reason']) for failure in response['failures']])
            )

        taskArns = [t['taskArn'] for t in response['tasks']]  # get task arn to track status

        self.wait_task(cluster, taskArns)

        success = self.get_task_status(cluster, taskArns)  # returns when stopped

        if success:
            print('There be {} successful task(s) with logs :)'.format(len(taskArns)))
        else:
            print('There be unsuccessful task(s) with logs :(')
    

        return taskArns, success, logs_stream_prefix, logs_group


if __name__ == '__main__':
    a = ECSInteraction()
    ta, success, sp, lg = a.run_task(cluster='Production-Services', task_definition='perry_qa', 
        command=["python3.6","-m","cases.306090.schedule"])

