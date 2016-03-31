'''===========================================================================
16-Oct-2015    josep.sampe    Initial implementation.
05-Feb-2016    josep.sampe    Added Proxy execution.
01-Mar-2016    josep.sampe    Addded pipeline (multi-node)
22-Mar-2016    josep.sampe    Enhanced performance
==========================================================================='''
from swift.common.swob import Request
from storlet_gateway.storlet_docker_gateway import StorletGatewayDocker
import json


class SDSGatewayStorlet():

    def __init__(self, conf, logger, app, v, account, container, obj, method):
        self.conf = conf
        self.logger = logger
        self.app = app
        self.version = v
        self.account = account
        self.container = container
        self.obj = obj
        self.gateway = None
        self.storlet_metadata = None
        self.storlet_name = None
        self.method = method
        self.server = self.conf['execution_server']
        self.gateway_method = None

    def set_storlet_request(self, req_resp, params):

        self.gateway = StorletGatewayDocker(self.conf, self.logger, self.app,
                                            self.version, self.account,
                                            self.container, self.obj)

        self.gateway_method = getattr(self.gateway, "gateway" +
                                      self.server.title() +
                                      self.method.title() + "Flow")

        # Set the Storlet Metadata to storletgateway
        md = {}
        md['X-Object-Meta-Storlet-Main'] = self.storlet_metadata['main']
        md['X-Object-Meta-Storlet-Dependency'] = self.storlet_metadata['dependencies']
        md['Content-Length'] = self.storlet_metadata['content_length']
        md['ETag'] = self.storlet_metadata['etag']
        self.gateway.storlet_metadata = md
        
        # Simulate Storlet request
        new_env = dict(req_resp.environ)
        req = Request.blank(new_env['PATH_INFO'], new_env)
        req.headers['X-Run-Storlet'] = self.storlet_name
        self.gateway.augmentStorletRequest(req)
        req.environ['QUERY_STRING'] = params.replace(',', '&')

        return req

    def launch_storlet(self, req_resp, params, input_pipe=None):
        req = self.set_storlet_request(req_resp, params)

        (_, app_iter) = self.gateway_method(req, self.container,
                                            self.obj, req_resp,
                                            input_pipe)

        return app_iter.obj_data, app_iter

    def execute_storlet(self, req_resp, storlet_list, storlet_md):
        out_fd = None
        storlet_executed = False
        on_other_server = {}
        
        # Execute multiple Storlets, PIPELINE, if any.
        for key in sorted(storlet_list):

            storlet = storlet_list[key]["storlet"]
            params = storlet_list[key]["params"]
            server = storlet_list[key]["execution_server"]
            storlet_id = storlet_list[key]["id"]

            self.storlet_name = storlet
            self.storlet_metadata = storlet_md[storlet]

            if server == self.server:
                self.logger.info('SDS Storlets - Go to execute ' + storlet +
                                 ' storlet with parameters "' + params + '"')

                out_fd, app_iter = self.launch_storlet(req_resp,
                                                       params, 
                                                       out_fd)
                storlet_executed = True
            else:
                storlet_execution = {'storlet': storlet,
                                     'params': params,
                                     'execution_server': server,
                                     'id': storlet_id}
                on_other_server[key] = storlet_execution
        
        if on_other_server:
            req_resp.headers['SDS-IOSTACK'] = json.dumps(on_other_server)
        
        if storlet_executed:
            if isinstance(req_resp, Request):
                req_resp.environ['wsgi.input'] = app_iter
            else:
                req_resp.app_iter = app_iter
        
        return req_resp
