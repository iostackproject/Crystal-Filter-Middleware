from setuptools import setup

paste_factory = ['sds_storlet_handler = '
                 'swift_sds_storlets.sds_storlet_handler:filter_factory']

setup(name='swift_sds_storlets',
      version='0.0.3',
      description='SDS - Storlet interceptor module for OpenStack Swift',
      author='The AST-IOStack Team',
      url='http://iostack.eu',
      packages=['swift_sds_storlets'],
      requires=['swift(>=1.4)','storlets(>=1.0)'],
      entry_points={'paste.filter_factory':paste_factory}
      )
