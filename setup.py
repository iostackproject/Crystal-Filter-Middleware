from setuptools import setup

paste_factory = ['sds_handler = '
                 'swift_sds.sds_handler:filter_factory']

setup(name='swift_sds',
      version='0.0.2',
      description='SDS - Storlet interceptor module for OpenStack Swift',
      author='The AST-IOStack Team',
      url='http://iostack.eu',
      packages=['swift_sds'],
      requires=['swift(>=1.4)'],
      install_requires=['swift_sds>=0.0.1'],
      entry_points={'paste.filter_factory':paste_factory}
      )
