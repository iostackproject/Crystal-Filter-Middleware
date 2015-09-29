import os
from swift.common.swob import wsgify, HTTPUnauthorized, HTTPBadRequest
from swift.common.utils import get_logger, split_path
from swift.proxy.controllers.base import get_account_info, get_container_info


class StorletMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        # host = conf.get('stacksync_host', '127.0.0.1').lower()
        # port = conf.get('stacksync_port', 61234)
        self.app.logger = get_logger(conf, log_route='storlet_middleware')
        self.app.logger.info('Storlet middleware: Init OK')

        '''
        self.user_filter is a hashmap that stores the relation between user
        and storlet.
        This hashmap should be changed it for Memcached system.
        '''
        self.user_filter = {}
        self.user_filter["AUTH_4f0279da74ef4584a29dc72c835fe2c9"]="UOneTrace-1.0.jar"

    @wsgify
    def __call__(self, req):

        self.app.logger.info('Storlet middleware: __call__: %r', req.environ)

        '''
        Using split_path we can obtain the account id.
        '''
        _, account, container, swift_object = split_path(req.path, 0, 4, True)

        '''
        We only add the storlets headers if the call goes to the swift object.
        '''
        if not swift_object:
            return self.app
        self.app.logger.info('Storlet middleware: Account INFO: Start')

        #check if is a valid request
        if not self.valid_request(req):
            # We only want to process PUT, POST and GET requests
            return self.app
        '''
        This is the core part of the middleware. Here we should consult to a
        Memcached/DB about the user. If this user has some storlet activated,
        we need to add the storlet headers including the parameters defined by
        the user. (now we are consulting the hashmap defined in the init function)
        '''
	if account in self.user_filter.keys():
	    req.headers["X-Run-Storlet"]=self.user_filter[account]


        return self.app

    def valid_request(self, req):
        self.app.logger.info('Storlet middleware: Valid req')
        if (req.method == 'PUT' or req.method == 'GET' or req.method == 'POST'):
            #Also we need to discard the copy calls.
            if not "HTTP_X_COPY_FROM" in req.environ.keys():
                self.app.logger.info('Storlet middleware: Valid req: OK')
                return True
            return False
        return False


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    def storlets_filter(app):
        return StorletMiddleware(app, conf)

    return storlets_filter
