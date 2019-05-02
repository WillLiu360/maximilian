"""
Provides basic logging functionality
"""

import logging
from cocore.config import Config
import inspect
from datetime import datetime
import os, sys
import traceback


class Logger:
    """
    generic logger class
    """
    def __init__(self, logname=None, project_name=None):
        """
        :param level:
        :param project_name: if provided the logger will put its log files in a subdirectory named after the project
        :return:
        """
        conf = Config()

        parent = inspect.stack()[1]
        parent_file =  inspect.getfile(parent[0])
        path, path_filename = os.path.split(parent_file)
        if logname:
            filename = logname
        else:
            filename = path_filename
        if project_name:
            log_path = 'logs/' + project_name + '/' + filename
        else:
            log_path = 'logs/' + filename

        if project_name:
            if not os.path.exists('logs/' + project_name + '/'):
                os.makedirs('logs/' + project_name + '/')
        else:
            if not os.path.exists('logs/'):
                os.makedirs('logs/')

        log_name = datetime.now().strftime(log_path + "-%Y%m%d-%H%M%S.log")
        l = logging.getLogger("test")
        l.setLevel('DEBUG')
        l_fh = logging.FileHandler(log_name)
        l_format = logging.Formatter('%(asctime)s %(message)s')
        l_fh.setFormatter(l_format)
        l.addHandler(l_fh)
        self.logger = l

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
