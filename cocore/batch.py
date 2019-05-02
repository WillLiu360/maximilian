from cocore.config import Config
import requests
import os, sys
from pprint import pprint

class Batch(object):
    """
    integrated integration to Batchy batch control system
    """
    def __init__(self, wf):
        self.wf = wf
        try:
            server = Config()['batchy']['server']
        except:
            server = '0.0.0.0'
        try:
            port = Config()['batchy']['port']
        except:
            port = '8050'
        # different URL if behind load balancer
        if "hypergate" in server and port == '80':
            self.server_url = 'http://' + server + '/batchy'
        else:
            self.server_url = 'http://' + server + ':' + port

    def open(self, fmt=None):
        """
        opens batch and returns batch params, by default batch params will be returned as python objects
        :param wf:
        :param fmt: infa, json (default is python object)
        :return:
        """
        r = None
        if fmt == 'json':
            r = requests.get(self.server_url + '/open_batch/' + self.wf).text()
        elif fmt == 'infa':
            r = requests.get(self.server_url + '/open_batch/infa/' + self.wf).text()
        else:
            r = requests.get(self.server_url + '/open_batch/' + self.wf).json()
        return r

    def close(self):
        """
        close batch
        :param wf:
        :return:
        """
        r = requests.get(self.server_url + '/close_batch/' + self.wf).json()
        return r

    def fail(self):
        """
        fail batch
        :param wf:
        :return:
        """
        r = requests.get(self.server_url + '/fail_batch/' + self.wf).json()
        return r

    def get_status(self):
        """
        get status
        :param wf:
        :return:
        """
        r = requests.get(self.server_url + '/get_status/' + self.wf).json()
        return r

    def get_status_summary(self):
        """
        get a single value for the wole workflow
        :return:
        """
        details = self.get_status()
        r = {'batch_id': None, 'status': 'success', 'failure_cnt': 0, 'open_cnt': 0, 'batch_start': None, 'batch_end': None}
        for k,x in details.items():
            if  x.get('status') == 'failure':
                r['failure_cnt'] += 1
            if  x.get('status') == 'open':
                r['open_cnt'] += 1
            r['batch_start'] = x.get('batch_start') if r['batch_start'] is None or x.get('batch_start') <  r['batch_start'] else  r['batch_start']
            r['batch_end'] = x.get('batch_end') if r['batch_end'] is None or x.get('batch_end') > r['batch_end'] else r['batch_end']
            r['batch_id'] = x.get('batch_id')
        if r['failure_cnt'] > 0:
            r['status'] = 'failure'
        elif r['open_cnt'] > 0:
            r['status'] = 'open'
        return r

# b = Batch('wf1')
# # pprint(b.get_status())
#
# print b.get_status_summary()


