from setuptools import setup
import storlets_swift

setup(name='storlets_swift',
      version=storlets_swift.__version__,
      description='Storlet interceptor module for OpenStack Swift',
      author='The AST-IOStack Team',
      url='http://iostack.eu',
      packages=['storlets_swift'],
      requires=['swift(>=1.4)'],
      install_requires=['storlets_swift>=0.0.1'],
      entry_points={'paste.filter_factory':
                        ['storlets_swift=storlets_swift.storlets_swift:filter_factory']})
