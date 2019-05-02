import argparse
import base64
import configparser
import os
import os.path
from abc import abstractmethod, ABCMeta


def find_config_path(file_name, search_root):
    """Return the absolute path the configuration file.

    Given the config filename, and a root to start the search, return the absolute path to the config
    file, or None if it can't be found.

    :param file_name: The configuration file name.
    :param search_root: The path root from which to search for the configuration file.
    :return: If exists, absolute path to the configuration file or None
    """
    abs_root_path = os.path.abspath(search_root)
    this_level = os.listdir(abs_root_path)

    if file_name in this_level:
        return os.path.join(abs_root_path, file_name)

    branches = [
        os.path.join(abs_root_path, branch_name)
        for branch_name in this_level
        if os.path.isdir(os.path.join(abs_root_path, branch_name))
    ]
    for branch in branches:
        found = find_config_path(file_name, branch)
        if found:
            return found
    return None


class MetaConfig(argparse.Action):
    __metaclass__ = ABCMeta
    options = []

    @staticmethod
    def _check_ordering(option):
        if option == '--config' and '--vars' in MetaConfig.options:
            print(
                "You supplied both --config and --vars options, but in a wonky order.  If you want to override config"
                " file options with command line options, make sure you order the options with --config first, then"
                " --vars.  In other words, parameters are overidden in the order they are specified on the "
                "command line."
            )
        MetaConfig.options.append(option)

    @staticmethod
    def _clean_key(key):
        """Replace hyphens in a key string with underscores

        :param key: String key
        :return: Cleaned string key
        """
        return key.replace('-', '_')

    def __call__(self, parser, namespace, values, option_string=None, *args, **kwargs):
        config = self.parse_config(values)
        if option_string == '--config':
            setattr(namespace, 'config_file', values)
        for key, value in config.items():
            setattr(namespace, self._clean_key(key), value)

    @abstractmethod
    def parse_config(self, config_path):  # pragma: no cover
        """Reads a configuration file and returns a dictionary of the configuration.

        :param config_path: Absolute path to configuration file.
        """
        raise NotImplementedError


class TextConfig(MetaConfig):
    def parse_config(self, config_path='etl.cfg'):
        """Reads a text configuration file and returns a dictionary of the configuration.

        Text configuration files are expected to end in .cfg or .txt

        :param config_path: Absolute path to configuration file.
        :return: Dictionary with configuration arguments and values
        """
        parser = configparser.ConfigParser()
        parser.read(config_path)
        dictionary = {}
        for section in parser.sections():
            dictionary[section] = {}
            for option in parser.options(section):
                val = parser.get(section, option)
                if val.isdigit():
                    val = int(val)
                if str(option).lower() in ('pwd', 'password'):
                    b_val = base64.b64decode(val)
                    val = str(b_val, 'utf-8')
                dictionary[section][option] = val
        return dictionary


# noinspection PyPep8Naming
def Config(conf_path='etl.cfg'):
    """Reads config file and returns a dictionary of config objects.

    Deprecated: Should use TextConfig class instead.
    Also handles basic password decryption.

    :param conf_path: Absolute path to configuration file.
    :return: Dictionary with configuration arguments and values
    """
    config = configparser.ConfigParser()
    config.read(conf_path)
    dictionary = {}
    for section in config.sections():
        dictionary[section] = {}
        for option in config.options(section):
            val = config.get(section, option)
            if str(option).lower() in ('pwd', 'password'):
                val = base64.b64decode(val)
            dictionary[section][option] = val
    for section in config.sections():
        dictionary[section] = {}
        for option in config.options(section):
            val = config.get(section, option)
            if val.isdigit():
                val = int(val)
            if str(option).lower() in ('pwd', 'password'):
                b_val = base64.b64decode(val)
                val = str(b_val, 'utf-8')
            dictionary[section][option] = val
    return dictionary
