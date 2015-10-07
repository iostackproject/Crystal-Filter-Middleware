import os
from swift.common.swob import wsgify, HTTPUnauthorized, HTTPBadRequest
from swift.common.utils import get_logger, split_path
from swift.proxy.controllers.base import get_account_info, get_container_info
import redis

class StorletMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        # host = conf.get('stacksync_host', '127.0.0.1').lower()
        # port = conf.get('stacksync_port', 61234)
        self.app.logger = get_logger(conf, log_route='storlet_middleware')
        self.app.logger.info('Storlet middleware: Init OK')

        '''
        self.user_filter is a hashmap that stores the relation between user
        and storlet and also the parameters of the storlet.
        This hashmap should be changed it for Memcached system.
        '''
        self.user_filter = {}
        self.user_filter["AUTH_4f0279da74ef4584a29dc72c835fe2c9"]="UOneTrace-1.0.jar"
	self.redis_connection = redis.StrictRedis(host='10.30.103.250', port=16379, db=0)
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

        if not self.valid_container(container):
            return self.app
        #check if is a valid request
        if not self.valid_request(req):
            # We only want to process PUT, POST and GET requests
            # Also we ignore the calls that goes to the storlet and dependency container
            return self.app

        '''
        This is the core part of the middleware. Here we should consult to a
        Memcached/DB about the user. If this user has some storlet activated,
        we need to add the storlet headers including the parameters defined by
        the user. (now we are consulting the hashmap defined in the init function)
        '''
	storlet_list = self.redis_connection.lrange(str(account), 0, -1)

        self.app.logger.info('Storlet middleware: storlet_info: '+str(storlet_list))
	for storlet in storlet_list:
            params = self.redis_connection.hgetall(str(account)+":"+str(storlet))
	    self.app.logger.info('Storlet middleware: params: '+str(params))
	    req.headers["X-Run-Storlet"]=storlet
            self.app.logger.info('Storlet middleware: header: '+str(req.headers))

        return self.app

    def valid_request(self, req):
        self.app.logger.info('Storlet middleware: Valid req')
        if (req.method == 'PUT' or req.method == 'GET' or req.method == 'POST'):
            #Also we need to discard the copy calls.
            if not "HTTP_X_COPY_FROM" in req.environ.keys():
                self.app.logger.info('Storlet middleware: Valid req: OK')
                return True
        return False
    def valid_container(self, container):
        if container != "storlet" and container != "dependency":
            return True
        return False


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    def storlets_filter(app):
        return StorletMiddleware(app, conf)

    return storlets_filter

