import pkg_resources

__path__ = __import__('pkgutil').extend_path(__path__, __name__)
__version__ = pkg_resources.get_distribution('coco.core').version
