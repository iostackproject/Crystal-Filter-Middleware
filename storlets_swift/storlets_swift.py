from swift.common.utils import get_logger, is_success, cache_from_env

import os
from swift.common.swob import wsgify, HTTPUnauthorized, HTTPBadRequest, Response, Request
from swift.common.utils import get_logger, split_path
from swift.proxy.controllers.base import get_account_info, get_container_info
from controller_common import put_metadata, get_metadata
import redis
import ConfigParser
import controller_storlet_gateway as csg
import json

class StorletMiddleware(object):
    def __init__(self, app, conf):
        self.app = app
        # host = conf.get('stacksync_host', '127.0.0.1').lower()
        # port = conf.get('stacksync_port', 61234)
        self.execution_server = conf.get('execution_server', 'proxy')
        self.app.logger = get_logger(conf, log_route='storlet_middleware')
        self.hconf = conf
        self.containers = [conf.get('handler_container'),
                           conf.get('handler_dependency')]
        #Redis  connection
        self.redis_connection = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
        self.app.logger.debug('Storlet middleware: Init OK')

    @wsgify
    def __call__(self, req):

        self.app.logger.info('Storlet middleware: __call__: %r', req.environ)
        # Request part
        '''
        Using split_path we can obtain the account id.
        '''
        if self.execution_server == 'proxy':

            version, account, container, obj = req.split_path(2, 4, rest_with_last=True)
            self.app.logger.info('Storlet middleware: proxy-execution: ')

            if not self.valid_request(req, container):
                return self.app

            if req.method == "PUT":

                '''
                This is the core part of the middleware. Here we should consult to a
                Memcached/DB about the user. If this user has some storlet activated,
                we need to add the storlet headers including the parameters defined by
                the user. (now we are consulting the hashmap defined in the init function)
                '''
                storlet_list = self.redis_connection.lrange(str(account), 0, -1)
                self.app.logger.info('Storlet middleware: storlet_list: %r ', storlet_list)
                if not storlet_list:
                    self.app.logger.info('Storlet middleware: NOT STORLET LIST')
                    return self.app

                storlet_gateway = csg.ControllerGatewayStorlet(self.hconf, self.app.logger, self.app, account,
                                                                container, obj)

                account_meta = storlet_gateway.get_account_info()
                out_fd = None
                toProxy = 0
                toObject = 0

                # Verify if the account can execute Storlets
                storlets_enabled = account_meta.get('x-account-meta-storlet-enabled','False')

                if storlets_enabled == 'False':
                    self.app.logger.info('Swift Controller - Account disabled for storlets')
                    return HTTPBadRequest('Swift Controller - Account disabled for storlets')

                
                self.app.logger.info('Storlet middleware: PUT: '+str(storlet_list))
                for storlet in storlet_list:
                    storlet_metadata = self.redis_connection.hgetall(str(account)+":"+str(storlet))
                    self.app.logger.info('Storlet middleware: PUT: '+str(storlet_metadata))
                    # if put storlet
                    if storlet_metadata["PUT"] == "True":
                        self.app.logger.debug('Storlet middleware: params: '+str(storlet_metadata))
                        if storlet_metadata["executor_node"] == "proxy":

                            if not storlet_gateway.authorize_storlet_execution(storlet):
                                return HTTPUnauthorized('Swift Controller - Storlet: No permission')

                            # execute the storlet
                            old_env = req.environ.copy()
                            orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                            #TODO: Review this function that can not be executed in the request part
                            params = storlet_metadata["params"]
                            self.app.logger.info('Storlet middleware: PARAMS: '+str(params))
                            app_iter = storlet_gateway.execute_storlet_on_proxy_put(req, params,out_fd)
                            req.headers["Storlet-Executed"] = "True"
                            req.environ['wsgi.input'] = app_iter

                            if 'CONTENT_LENGTH' in req.environ:
                                req.environ.pop('CONTENT_LENGTH')

                            req.headers['Transfer-Encoding'] = 'chunked'

                        else:
                            req.headers["Storlet-Execute-On-Object-"+str(toProxy)] = storlet
                            req.headers["Storlet-Execute-On-Object-Parameters-"+str(toProxy)] = parameters
                            toObject = toObject + 1
                            req.headers["Total-Storlets-To-Execute-On-Object"] = toObject

                        self.app.logger.debug('Storlet middleware: headers: '+str(req.headers))

                return self.app

        else:
            device, partition, account, container, obj = req.split_path(5, 5, rest_with_last=True)
            version = '0'

        # Response part
        orig_resp = req.get_response(self.app)

        # The next part of code is only executed by the object servers
        if self.execution_server == 'object':
            self.app.logger.info('Swift middleware - Object Server execution')

            if not self.valid_request(req, container):
                return orig_resp

            if req.method == "GET":
                """
                If orig_resp is GET we will need to:
                - Take the object metadata info
                - Execute the storlets described in the metadata info
                - Execute the storlets described in redis
                - Return the result
                """
                object_metadata = get_metadata(orig_resp)
                if not object_metadata: # Any storlet to execute, return the response
                    return orig_resp

                storlet_gateway = csg.ControllerGatewayStorlet(self.hconf, self.app.logger, self.app, account,
                                                                container, obj)
                account_meta = storlet_gateway.get_account_info()
                out_fd = None
                toProxy = 0

                # Verify if the account can execute Storlets
                storlets_enabled = account_meta.get('x-account-meta-storlet-enabled','False')

                if storlets_enabled == 'False':
                    self.app.logger.info('Swift Controller - Account disabled for storlets')
                    return HTTPBadRequest('Swift Controller - Account disabled for storlets')

                # Execute the storlets described in the metadata info
                for storlet in reversed(object_metadata):
                    # execute each storlet in the correct order
                    # TODO: Take from redis where execute the storlet (proxy or object server) Take the parameters from redis?
                    if node_to_execute == "object_server":

                        if not storlet_gateway.authorize_storlet_execution(storlet):
                            return HTTPUnauthorized('Swift Controller - Storlet: No permission')

                        old_env = req.environ.copy()
                        orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                        out_fd, app_iter = storlet_gateway.executeStorletOnObject(orig_resp,parameters+"&reverse=True",out_fd)

                        orig_resp.headers["Storlet-Executed"] = "True"

                    else:
                        orig_resp.headers["Storlet-Execute-On-Proxy-"+str(toProxy)] = storlet
                        orig_resp.headers["Storlet-Execute-On-Proxy-Parameters-"+str(toProxy)] = parameters
                        toProxy = toProxy + 1
                        orig_resp.headers["Total-Storlets-To-Execute-On-Proxy"] = toProxy

                # Execute the storlets described in redis
                for index in range(int(orig_resp.headers["Total-Storlets-To-Execute-On-Object"])):
                    storlet = orig_resp.headers["Storlet-Execute-On-Object-"+str(index)]
                    parameters = orig_resp.headers["Storlet-Execute-On-Object-Parameters-"+str(index)]

                    if not storlet_gateway.authorize_storlet_execution(storlet):
                        return HTTPUnauthorized('Swift Controller - Storlet: No permission')

                    old_env = req.environ.copy()
                    orig_req = Request.blank(old_env['PATH_INFO'], old_env)
                    out_fd, app_iter = storlet_gateway.executeStorletOnObject(orig_resp, parameters, out_fd)

                    orig_resp.headers["Storlet-Executed"] = "True"

                # Delete headers for the correct working of the Storlet framework
                if 'Content-Length' in orig_resp.headers:
                    orig_resp.headers.pop('Content-Length')
                if 'Transfer-Encoding' in orig_resp.headers:
                    orig_resp.headers.pop('Transfer-Encoding')

                # Return Storlet response
                return Response(app_iter=app_iter,
                                headers=orig_resp.headers,
                                request=orig_req,
                                conditional_response=True)

            if req.method == "PUT":
                """
                If orig_resp is PUT we will need to:
                - Take storlets executed in the proxy from headers
                - Generate a GET copy
                - Save the storlets executed into the object metadata
                """
                #TODO: This part needs information not setted yet
                # convert put to get, to obtain the object metadata
                get_req = req.copy_get()
                get_resp = get_req.get_response(self.app)
                storlet_executed_list = []

                # Take all the storlets executed in the request part in the proxy
                for index in range(int(orig_resp.headers["Total-Storlets-To-Execute-On-Proxy"])):
                    self.app.logger.info('Storlet middleware: storlets to write in object metadata')
                    storlet = orig_resp.headers["Storlet-Execute-On-Proxy-"+str(index)]
                    parameters = orig_resp.headers["Storlet-Execute-On-Proxy-Parameters-"+str(index)]
                    storlet_dictionary = {"storlet_name":storlet, "params":parameters, "execution_server":"proxy"}
                    storlet_executed_list.append(storlet_dictionary)

                # Save the storlets executed into the object metadata
                if not storlet_executed_list:
                    return orig_resp

                if not put_metadata(get_resp, storlet_executed_list):
                    #TODO: Rise exception writting metadata
                    return orig_resp

            return orig_resp

    def valid_request(self, req, container):
        # We only want to process PUT, POST and GET requests
        # Also we ignore the calls that goes to the storlet, dependency and docker_image container
        if (req.method == 'GET' or req.method == 'PUT')   and container not in self.containers:
            #Also we need to discard the copy calls.
            if not "HTTP_X_COPY_FROM" in req.environ.keys():
                self.app.logger.info('Swift Controller - Valid req: OK!')
                return True

        self.app.logger.info('Swift Controller - Valid req: NO!')
        return False


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)


    mc_conf = dict()
    mc_conf['execution_server'] = conf.get('execution_server','object')
    mc_conf['controller_timeout'] = conf.get('controller_timeout', 20)
    mc_conf['controller_pipe'] = conf.get('controller_pipe',
                                          'controller_pipe')
    mc_conf['storlet_timeout'] = conf.get('storlet_timeout',40)
    mc_conf['storlet_container'] = conf.get('storlet_container','storlet')
    mc_conf['storlet_dependency'] = conf.get('storlet_dependency',
                                             'dependency')

    mc_conf['docker_repo'] = conf.get('docker_repo','10.30.239.240:5001')

    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('storlet_gateway_conf',
                               '/etc/swift/storlet_docker_gateway.conf'))

    additional_items = configParser.items("DEFAULT")
    for key, val in additional_items:
        mc_conf[key] = val

    def storlets_filter(app):
        return StorletMiddleware(app, mc_conf)

    return storlets_filter
