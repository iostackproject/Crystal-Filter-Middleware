'''===========================================================================
16-Oct-2015    josep.sampe    Initial implementation.
05-Feb-2016    josep.sampe    Added Proxy execution.
01-Mar-2016    josep.sampe    Addded pipeline (multi-node)
22-Mar-2016    josep.sampe    Enhanced performance
==========================================================================='''
from storlet_gateway.storlet_docker_gateway import StorletGatewayDocker
from swift.common.swob import Request

class SDSGatewayStorlet():

    def __init__(self, conf, logger, request_data):
        self.conf = conf
        self.logger = logger
        self.app = request_data['app']
        self.version = request_data['api_version']
        self.account = request_data['account']
        self.container = request_data['container']
        self.obj = request_data['object']
        self.gateway = None
        self.storlet_metadata = None
        self.storlet_name = None
        self.method = request_data['method']
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
        md['Content-Length'] = self.storlet_metadata['size']
        #md['ETag'] = self.storlet_metadata['etag']
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

        return app_iter

    def execute_storlet(self, req_resp, storlet_data, app_iter):
        storlet = storlet_data['name']
        params = storlet_data['params']
        self.storlet_name = storlet
        self.storlet_metadata = storlet_data

        self.logger.info('Crystal Filters - Go to execute ' + storlet +
                         ' storlet with parameters "' + params + '"')
                
        app_iter = self.launch_storlet(req_resp, params, app_iter)
        
        return app_iter
