import gevent.monkey
gevent.monkey.patch_all()

import time
import requests
from cocore.esLogger import Logger
import xml.etree.cElementTree as et

l = Logger(job_name='infa_cloud_job_runner.py')


class InformaticaCloudInteraction:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.icSessionId = ""
        self.serverUrl = ""
        self.activity_log_api = ''
        self.job_api = ''
        self.login_api = "https://app.informaticaondemand.com/ma/api/v2/user/login"
        self.logout_api = ''

    def login(self):
        headers = {
            'Accept': 'application/xml',
            'Content-Type': 'application/xml'
        }
        xml = "<login><username>{}</username><password>{}</password></login>".format(self.username, self.password)
        r = requests.post(self.login_api, data=xml, headers=headers)
        response_xml = et.fromstring(r.text)
        self.serverUrl = response_xml.find('serverUrl').text
        self.job_api = f'{self.serverUrl}/api/v2/job'
        print(f'serverUrl: ' + self.serverUrl)
        try:
            self.icSessionId = response_xml.find('icSessionId').text
            l.l("login with sessionId " + self.icSessionId)
        except:
            print(r.text)
            self.icSessionId = ""

    def logout(self, icSessionId = None):
        if icSessionId:
            self.icSessionId = icSessionId
        if self.icSessionId:
            headers = {
                'Accept': 'application/xml',
                'Content-Type': 'application/xml',
                'icSessionId': self.icSessionId
            }
            self.logout_api = f'{self.serverUrl}/saas/api/v2/user/logout'
            r = requests.post(self.logout_api, data='', headers=headers)
            self.icSessionId = ""
            l.l("logout")

    def run_job_async(self, job_name, job_type):
        if self.icSessionId:
            headers = {
                'Accept': 'application/xml',
                'Content-Type': 'application/xml',
                'icSessionId': self.icSessionId
            }
            xml = "<job><taskName>{}</taskName><taskType>{}</taskType></job>".format(job_name, job_type)
            r = requests.post(self.job_api, data=xml, headers=headers)
            response_xml = et.fromstring(r.text)
            try:
                runId = response_xml.find('runId').text
                taskId = response_xml.find('taskId').text
                taskType = response_xml.find('taskType').text
                l.l("successfully started " + job_name + " with runId=" + runId)
                return taskId, runId, taskType
            except:
                l.l_error("error running " + job_name)
                l.l_error(r.text)
                #self.logout()
                return None, None, None

    def run_status_check(self, taskId, runId, current_status="0", current_errors=0):
        if current_status != "0":
            return taskId, runId, current_status, current_status
        if self.icSessionId:
            headers = {
                'Accept': 'application/xml',
                'Content-Type': 'application/xml',
                'icSessionId': self.icSessionId
            }
            failed_rows = -1
            query_params = {'taskId': taskId, 'runId': runId}
            l.l("running status check for " + taskId + " runId " + runId)
            try:
                self.activity_log_api = f'{self.serverUrl}/api/v2/activity/activityLog'
                r = requests.get(self.activity_log_api, params=query_params, headers=headers)
            except:
                return "-1"
            response_xml = et.fromstring(r.text)
            try:
                job_status = response_xml.find('activityLogEntry/state').text
            except:
                job_status = "0"
            try:
                failed_rows = response_xml.find('activityLogEntry/failedTargetRows').text
            except:
                failed_rows = "-1"
            l.l("got status " + job_status + " with " + failed_rows + " failed rows")
            return taskId, runId, job_status, failed_rows

    def __job_list_complete(self, job_set):
        running_jobs = [(taskId, runId, status, errors) for (taskId, runId, status, errors) in job_set if status == "0"]
        if len(running_jobs) > 0:
            return False
        else:
            return True

    def run_job_list(self, jobs):
        """

        :param jobs: array of tuples of (job_name, job_type)
        :return:    1 if jobs all complete successfully
                    2 if any job completes with errors
                    3 if any job can't finish
        """
        # generate a list of pointers to infa cloud jobs
        job_list = [self.run_job_async(job_name, job_type) for (job_name, job_type) in jobs]
        job_status = [(taskId, runId, "0", 0) for (taskId, runId, taskType) in job_list if runId is not None]
        sleep = 8
        while not self.__job_list_complete(job_status):
            time.sleep(sleep)
            job_status_check = [self.run_status_check(taskId, runId, status, errors) for (taskId, runId, status, errors) in job_status]
            job_status = job_status_check
            if sleep < 60:
                sleep = sleep * 2
            else:
                sleep = 60
        print("ending job_status")
        print(job_status)
        return job_status

    def run_job(self, job_name, job_type):
        taskId, runId, taskType = self.run_job_async(job_name, job_type)
        job_status = "0"
        failed_rows = -1
        if runId is not None:
            sleep = 8
            while job_status == "0":
                time.sleep(sleep)
                taskId, runId, job_status, failed_rows = self.run_status_check(taskId, runId)
                if sleep < 60:
                    sleep = sleep * 2
                else:
                    sleep = 60
            if job_status == "1":
                l.l("finished running job " + job_name)
            elif job_status == "2":
                l.l_error("job " + job_name + " finished with errors")
            elif job_status == "3":
                l.l_error("job " + job_name + " could not finish")
            elif job_status == "-1":
                l.l_error("there was a serious problem checking job " + job_name)
        return job_status, failed_rows
