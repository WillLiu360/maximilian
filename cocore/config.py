import base64
# noinspection PyCompatibility
from configparser import ConfigParser
import os
import os.path

from future import standard_library
standard_library.install_aliases()
from future.builtins import str


def find_config_path(file_name, search_root):
    """
    Given the config filename, and a root to start
    the search, return the absolute path to the config
    file, or None if it can't be found.
    """
    abs_root_path = os.path.abspath(search_root)
    this_level = os.listdir(abs_root_path)
    if file_name in this_level:
        return os.path.join(abs_root_path, file_name)

    branches = [
        os.path.abspath(branch_name)
        for branch_name in this_level
        if os.path.isdir(os.path.abspath(branch_name))]
    for branch in branches:
        found = find_config_path(file_name, branch)
        if found:
            return found
    return None


# noinspection PyPep8Naming
def Config(conf_path='etl.cfg'):
    """Reads config file and returns a dictionary of config objects.

    :param conf_path: Absolute path to configuration file.
    :return: Dictionary with configuration arguments and values
    """
    config = ConfigParser()
    config.read(conf_path)
    dictionary = {}
    for section in config.sections():
        dictionary[section] = {}
        for option in config.options(section):
            val = config.get(section, option)
            if str(option).lower() in ('pwd', 'password'):
                val = str(base64.b64decode(val), 'utf-8')
            dictionary[section][option] = val
    return dictionary
