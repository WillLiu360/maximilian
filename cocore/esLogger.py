import logging
# import requests
import grequests
import datetime
import json
from cocore.config import Config
import traceback
import sys

conf = Config()

es_endpoint = conf['es']['endpoint']
debug = conf['es']['debug']


def exception_handler(request, exception):
    print("Logging failed")


class esHandler(logging.Handler):
    def emit(self, record):
        log_entry = self.format(record)
        urls = [es_endpoint]
        grs = (grequests.post(u, data=log_entry) for u in urls)
        return grequests.map(grs, exception_handler=exception_handler)


class LogstashFormatter(logging.Formatter):
    def __init__(self, task_name=None):
        self.task_name = task_name

        super(LogstashFormatter, self).__init__()

    def format(self, record):
        data = {'message': record.msg,
                'timestampUTC': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                'type': 'log',
                }

        if self.task_name:
            data['job_name'] = self.task_name

        return json.dumps(data)


class Logger(object):
    """
    elasticsearch logger class
    """
    def __init__(self, job_name=None, project_name=None):
        """
        :param level:
        :return:
        """
        formatter = LogstashFormatter(task_name=job_name)
        handler = esHandler()
        # handler = logging.NullHandler()
        handler.setFormatter(formatter)

        logger = logging.getLogger(job_name)
        if debug == '1':
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.propagate = False
        self.logger = logger

    def l_error(self, msg):
        self.l(msg, level=logging.ERROR)

    def l_exception(self, msg='general exception'):
        """
        new logging method will log as error with full traceback
        :param msg:
        :param exception_obj:
        :return:
        """
        etype, ex, tb = sys.exc_info()
        tb_s = traceback.format_exception(etype, ex, tb)
        msg = msg + ':\n' + ' '.join(tb_s)
        self.l(msg, level=logging.ERROR)

    def l(self, msg, level=logging.INFO):
        """ Write a log message. Utilizes (default) '_logger'.

        :param msg: Data to write to log file (Can be anything ...)
        :type msg: string

        :param level: Default debug; info & error are supported.
        :type level: string

        :raises: RuntimeError if log level us unknown.
        """
        ## only print 'DEBUG' messages if overall log level is set to debug

        if level is logging.DEBUG:
            self.logger.debug(msg)
            if debug == "1":
                print(msg)
        if level is logging.INFO:
            self.logger.info(msg)
            print(msg)
        elif level is logging.ERROR:
            self.logger.error(msg)
            self.send_failure_email(msg)
            print(msg)
        else:
            pass  # raise RuntimeError("Log level: %s not supported" % level)

    def d(self, msg):
        self.l(msg, level=logging.DEBUG)

    def handle_exception(self, exc_type, exc_value, exc_traceback):
        """
        """
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        self.logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

        self.send_failure_email(''.join(traceback.format_tb(exc_traceback)))

    def send_failure_email(self, message):
        pass
