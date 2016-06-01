'''===========================================================================
29-Sep-2015    edgar.zamora    Initial implementation.
02-Mar-2016    josep.sampe     Code refactor, New functionalities
21-Mar-2016    josep.sampe     Improved performance
31-May-2016    josep.sampe     Storlet middleware -> Crystal filter middleware
==========================================================================='''
from swift.proxy.controllers.base import get_account_info
from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import config_true_value
from swift.common.utils import get_logger
from crystal_filter_control import CrystalFilterControl
import crystal_filter_common as sc
import ConfigParser
import mimetypes
import redis
import json


class NotSDSFilterRequest(Exception):
    pass


def _request_instance_property():
    """
    Set and retrieve the request instance.
    This works to force to tie the consistency between the request path and
    self.vars (i.e. api_version, account, container, obj) even if unexpectedly
    (separately) assigned.
    """

    def getter(self):
        return self._request

    def setter(self, request):
        self._request = request
        try:
            self._extract_vaco()
        except ValueError:
            raise NotSDSFilterRequest()

    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class BaseSDSFilterHandler(object):
    """
    This is an abstract handler for Proxy/Object Server middleware
    """
    request = _request_instance_property()

    def __init__(self, request, conf, app, logger, filter_control):
        """
        :param request: swob.Request instance
        :param conf: gateway conf dict
        """
        self.request = request
        self.server = conf.get('execution_server')
        self.sds_containers = [conf.get('storlet_container'),
                               conf.get('storlet_dependency')]
        self.app = app
        self.logger = logger
        self.conf = conf
        self.filter_control = filter_control
        
        self.redis_host = conf.get('redis_host')
        self.redis_port = conf.get('redis_port')
        self.redis_db = conf.get('redis_db')
        self.cache = conf.get('cache')
        
        self.method = self.request.method.lower()
        
        self.redis_connection = redis.StrictRedis(self.redis_host, 
                                                  self.redis_port, 
                                                  self.redis_db)
        

    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        self._api_version, self._account, self._container, self._obj = \
            self._parse_vaco()

    @property
    def api_version(self):
        return self._api_version

    @property
    def account(self):
        return self._account

    @property
    def container(self):
        return self._container

    @property
    def obj(self):
        return self._obj

    def _parse_vaco(self):
        """
        Parse method of path from self.request which depends on child class
        (Proxy or Object)
        :return tuple: a string tuple of (version, account, container, object)
        """
        raise NotImplementedError()

    def handle_request(self):
        """
        Run storlet
        """
        raise NotImplementedError()

    @property
    def is_storlet_execution(self):
        return 'X-Run-Storlet' in self.request.headers

    @property
    def is_range_request(self):
        """
        Determines whether the request is a byte-range request
        """
        return 'Range' in self.request.headers

    def is_available_trigger(self):
        return any((True for x in self.available_triggers
                    if x in self.request.headers.keys()))

    def is_slo_response(self, resp):
        self.logger.debug(
            'Verify if {0}/{1}/{2} is an SLO assembly object'.format(
                self.account, self.container, self.obj))
        is_slo = 'X-Static-Large-Object' in resp.headers
        if is_slo:
            self.logger.debug(
                '{0}/{1}/{2} is indeed an SLO assembly '
                'object'.format(self.account, self.container, self.obj))
        else:
            self.logger.debug(
                '{0}/{1}/{2} is NOT an SLO assembly object'.format(
                    self.account, self.container, self.obj))
        return is_slo

    def is_account_storlet_enabled(self):
        account_meta = get_account_info(self.request.environ,
                                        self.app)['meta']
        storlets_enabled = account_meta.get('storlet-enabled',
                                            'False')

        if not config_true_value(storlets_enabled):
            return True # TODO: CHANGE TO FALSE

        return True

    def _call_filter_control_on_put(self, filter_list):
        """
        Call gateway module to get result of filter execution
        in PUT flow
        """
        return self.filter_control.execute_filters(self.request, filter_list,
                                                   self.conf, self.logger, 
                                                   self.app, self._api_version, 
                                                   self.account, self.container, 
                                                   self.obj, self.method)

    def _call_filter_control_on_get(self, resp, filter_list):
        """
        Call gateway module to get result of filter execution
        in GET flow
        """
        return self.filter_control.execute_filters(resp, filter_list,
                                                   self.conf, self.logger, 
                                                   self.app, self._api_version, 
                                                   self.account, self.container, 
                                                   self.obj, self.method)

    def apply_filters_on_get(self, resp, filter_list):
        return self._call_filter_control_on_get(resp, filter_list)

    def apply_filters_on_put(self, filter_list):
        self.request = self._call_filter_control_on_put(filter_list)

        if 'CONTENT_LENGTH' in self.request.environ:
            self.request.environ.pop('CONTENT_LENGTH')
        self.request.headers['Transfer-Encoding'] = 'chunked'


class SDSFilterProxyHandler(BaseSDSFilterHandler):

    def __init__(self, request, conf, app, logger, filter_control):        
        super(SDSFilterProxyHandler, self).__init__(request, conf, 
                                                    app, logger,
                                                    filter_control)

        # Dynamic binding of policies
        account_key_list = self.redis_connection.keys("pipeline:"+
                                                      str(self.account)+ 
                                                      "*")

        self.filter_list = None
        key = self.account + "/" + self.container + "/" + self.obj
        for target in range(3):
            self.target_key = key.rsplit("/", target)[0]
            if 'pipeline:' + self.target_key in account_key_list:
                self.filter_list = self.redis_connection.hgetall(
                    'pipeline:' + self.target_key)
                break

    def _parse_vaco(self):
        return self.request.split_path(4, 4, rest_with_last=True)

    def _get_object_type(self):
        object_type = self.request.headers['Content-Type']
        if not object_type:
            object_type = mimetypes.guess_type(
                self.request.environ['PATH_INFO'])[0]
        return object_type

    def is_proxy_runnable(self, resp):
        # SLO / proxy only case:
        # storlet to be invoked now at proxy side:
        runnable = any(
            [self.is_range_request, self.is_slo_response(resp),
             self.conf['storlet_execute_on_proxy_only']])
        return runnable

    def check_size_type(self, filter_metadata):

        correct_type = True
        correct_size = True
        
        if filter_metadata['object_type']:
            obj_type = filter_metadata['object_type']
            correct_type = self._get_object_type() in \
                self.redis_connection.lrange("object_type:"+obj_type, 0, -1)
            
        if filter_metadata['object_size']:
            object_size = filter_metadata['object_size']
            op = sc.mappings[object_size[0]]
            obj_lenght = int(object_size[1])

            correct_size = op(int(self.request.headers['Content-Length']),
                              obj_lenght)

        return correct_type and correct_size

    @property
    def is_sds_object_put(self):
        return (self.container in self.sds_containers and self.obj and
                self.request.method == 'PUT')

    def handle_request(self):
        if self.is_sds_object_put:
            return self.request.get_response(self.app)
        elif self.is_account_storlet_enabled():
            if hasattr(self, self.request.method):
                resp = getattr(self, self.request.method)()
                return resp
            else:
                return self.request.get_response(self.app)
        else:
            self.logger.info('SDS Storlets - Account disabled for Storlets')
            return self.request.get_response(self.app)
        
    def _build_filter_execution_list(self):
        filter_execution_list = dict()
        
        for _, filter_metadata in self.filter_list.items():            
            filter_metadata = json.loads(filter_metadata)

            # Check conditions
            if filter_metadata["is_" + self.method]:
                if self.check_size_type(filter_metadata):
                    filter_name = filter_metadata['name']
                    server = filter_metadata["execution_server"]
                    reverse = filter_metadata["execution_server_reverse"]
                    params = filter_metadata["params"]
                    filter_id = filter_metadata["filter_id"]
                    filter_type = 'storlet' #filter_metadata["filter_type"]
                    filter_main = filter_metadata["main"]
                    filter_dependencies = filter_metadata["dependencies"]
                    filter_size = filter_metadata["content_length"]
                    has_reverse = filter_metadata["has_reverse"]
                    
                    filter_execution = {'name': filter_name,
                                        'params': params,
                                        'execution_server': server,
                                        'execution_server_reverse': reverse,
                                        'id': filter_id,
                                        'type': filter_type,
                                        'main': filter_main,
                                        'dependencies': filter_dependencies,
                                        'size': filter_size,
                                        'has_reverse': has_reverse}
                   
                    launch_key = filter_metadata["execution_order"]
                    filter_execution_list[launch_key] = filter_execution

        return filter_execution_list

    def GET(self):
        """
        GET handler on Proxy
        """    
        
        if self.filter_list:
            self.app.logger.info('Crystal Filters - ' + str(self.filter_list))
            filter_exec_list = self._build_filter_execution_list()
            self.request.headers['CRYSTAL-FILTERS'] = json.dumps(filter_exec_list)

        resp = self.request.get_response(self.app)
        
        if 'CRYSTAL-FILTERS' in resp.headers:
            self.logger.info('Crystal Filters - There are filters to execute '
                             'from object server')
            filter_exec_list = json.loads(resp.headers.pop('CRYSTAL-FILTERS'))
            return self.apply_filters_on_get(resp, filter_exec_list)

        return resp
    
    def PUT(self):
        """
        PUT handler on Proxy
        """
        if self.filter_list:
            self.app.logger.info('Crystal Filters - ' + str(self.filter_list))
            filter_exec_list = self._build_filter_execution_list()
            if filter_exec_list:
                self.request.headers['Filter-Executed-List'] = json.dumps(filter_exec_list)
                self.request.headers['Original-Size'] = self.request.headers.get('Content-Length','')
                self.request.headers['Original-Etag'] = self.request.headers.get('ETag','')
                
                if 'ETag' in self.request.headers:
                    # The object goes to be modified by some Storlet, so we
                    # delete the Etag from request headers to prevent checksum
                    # verification.
                    self.request.headers.pop('ETag')

                self.apply_filters_on_put(filter_exec_list)

            else:
                self.logger.info('Crystal Filters - No filters to execute')
        else:
            self.logger.info('Crystal Filters - No filters to execute')
        
        return self.request.get_response(self.app)


class SDSFilterObjectHandler(BaseSDSFilterHandler):

    def __init__(self, request, conf, app, logger,filter_control):
        super(SDSFilterObjectHandler, self).__init__(request, conf, 
                                                     app, logger,
                                                     filter_control) 
        
        self.device = self.request.environ['PATH_INFO'].split('/',2)[1]

    def _parse_vaco(self):
        _, _, acc, cont, obj = self.request.split_path(
            5, 5, rest_with_last=True)
        return ('0', acc, cont, obj)

    @property
    def is_slo_get_request(self):
        """
        Determines from a GET request and its  associated response
        if the object is a SLO
        """
        return self.request.params.get('multipart-manifest') == 'get'

    def handle_request(self):
        if hasattr(self, self.request.method):
            return getattr(self, self.request.method)()
        else:
            return self.request.get_response(self.app)
            # un-defined method should be NOT ALLOWED
            # return HTTPMethodNotAllowed(request=self.request)
         
    def _augment_filter_execution_list(self, filter_list):
        new_storlet_list = {}        
    
        # REVERSE EXECUTION
        if filter_list:            
            for key in reversed(sorted(filter_list)):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = filter_list[key]

        # Get filter list to execute from proxy
        if 'CRYSTAL-FILTERS' in self.request.headers:
            req_filter_list = json.loads(self.request.headers.pop('CRYSTAL-FILTERS'))

            for key in sorted(req_filter_list):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = req_filter_list[key]
        
        return new_storlet_list

    def _set_crystal_metadata(self):
        iostack_md = {}
        filter_exec_list = json.loads(self.request.headers['Filter-Executed-List'])
        iostack_md["original-etag"] = self.request.headers['Original-Etag']
        iostack_md["original-size"] = self.request.headers['Original-Size']
        iostack_md["filter-exec-list"] = filter_exec_list

        return iostack_md

    def GET(self):
        """
        GET handler on Object
        If orig_resp is GET we will need to:
        - Take the object metadata info
        - Execute the storlets described in the metadata info
        - Execute the storlets described in redis
        - Return the result
        """
        resp = self.request.get_response(self.app)
        
        iostack_md = sc.get_metadata(resp)
        
        if iostack_md:
            resp.headers['ETag'] = iostack_md['original-etag']
            resp.headers['Content-Length'] = iostack_md['original-size']
        
        filter_exec_list = self._augment_filter_execution_list(
                                 iostack_md.get('filter-exec-list',None))
        
        if filter_exec_list:
            return self.apply_filters_on_get(resp, filter_exec_list)
        
        return resp
               
    def PUT(self):
        """
        PUT handler on Object Server
        """
        # IF 'CRYSTAL-FILTERS' is in headers, means that is needed to run a
        # Filter on Object Server before store the object.
        if 'CRYSTAL-FILTERS' in self.request.headers:
            self.logger.info('Crystal Filters - There are filters to execute')
            filter_list = json.loads(self.request.headers['CRYSTAL-FILTERS'])
            self.apply_filters_on_put(filter_list)
        
        original_resp = self.request.get_response(self.app)
        
        # 'Storlet-List' header is the list of all Storlets executed, both 
        # on Proxy and on Object servers. It is necessary to save the list 
        # in the extended metadata of the object for run reverse-Storlet on 
        # GET requests.
        if 'Filter-Executed-List' in self.request.headers:
            crystal_metadata = self._set_crystal_metadata()
            if not sc.put_metadata(self.app, self.request, crystal_metadata):
                self.app.logger.error('Crystal Filters - Error writing'
                                      'metadata in an object')
                # TODO: Rise exception writting metadata
            # We need to restore the original ETAG to avoid checksum 
            # verification of Swift clients
            original_resp.headers['ETag'] = crystal_metadata['original-etag']
                
        return original_resp


class SDSFilterHandlerMiddleware(object):

    def __init__(self, app, conf, crystal_conf):
        self.app = app
        self.conf = crystal_conf
        self.logger = get_logger(conf, log_route='sds_storlet_handler')
        self.exec_server = self.conf.get('execution_server')
        self.containers = [self.conf.get('storlet_container'),
                           self.conf.get('storlet_dependency')]
        self.handler_class = self._get_handler(self.exec_server)
        
        ''' Singleton instance of filter control '''
        self.control_class = CrystalFilterControl
        self.filter_control =  self.control_class.Instance(conf = self.conf,
                                                           log = self.logger)
        
    def _get_handler(self, exec_server):
        if exec_server == 'proxy':
            return SDSFilterProxyHandler
        elif exec_server == 'object':
            return SDSFilterObjectHandler
        else:
            raise ValueError('configuration error: execution_server must'
                ' be either proxy or object but is %s' % exec_server)

    @wsgify
    def __call__(self, req):
        try:
            request_handler = self.handler_class(req, self.conf, 
                                                 self.app, self.logger,
                                                 self.filter_control)
            self.logger.debug('crystal_filter_handler call in %s: with %s/%s/%s' %
                              (self.exec_server, request_handler.account,
                               request_handler.container,
                               request_handler.obj))
        except HTTPException:
            raise
        except NotSDSFilterRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('Crystal filter middleware execution failed')
            raise
        except Exception:
            self.logger.exception('Crystal filter middleware execution failed')
            raise HTTPInternalServerError(body='Crystal filter middleware execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    
    conf = global_conf.copy()
    conf.update(local_conf)

    crystal_conf = dict()
    crystal_conf['execution_server'] = conf.get('execution_server', 'object')
    
    crystal_conf['redis_host'] = conf.get('redis_host', 'controller')
    crystal_conf['redis_port'] = conf.get('redis_port', 6379)
    crystal_conf['redis_db'] = conf.get('redis_db', 0)

    crystal_conf['storlet_timeout'] = conf.get('storlet_timeout', 40)
    crystal_conf['storlet_container'] = conf.get('storlet_container',
                                             'storlet')
    crystal_conf['storlet_dependency'] = conf.get('storlet_dependency',
                                              'dependency')
    crystal_conf['reseller_prefix'] = conf.get('reseller_prefix', 'AUTH')  
    crystal_conf['bind_ip'] = conf.get('bind_ip')
    crystal_conf['bind_port'] = conf.get('bind_port')


    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('storlet_gateway_conf',
                               '/etc/swift/storlet_docker_gateway.conf'))
    additional_items = configParser.items("DEFAULT")

    for key, val in additional_items:
        crystal_conf[key] = val

    def swift_sds_storlets(app):
        return SDSFilterHandlerMiddleware(app, conf, crystal_conf)

    return swift_sds_storlets
