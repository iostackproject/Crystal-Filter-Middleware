'''===========================================================================
29-Sep-2015    edgar.zamora    Initial implementation.
02-Mar-2016    josep.sampe     Code refactor (DRY, PEP8, ...)
==========================================================================='''
from swift.proxy.controllers.base import get_account_info
from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPBadRequest
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import config_true_value
from swift.common.utils import get_logger
import sds_storlet_gateway as vsg
import sds_common as sc
import ConfigParser
import mimetypes
import operator
import redis
import json
import time

mappings = {'>': operator.gt, '>=': operator.ge,
            '==': operator.eq, '<=': operator.le, '<': operator.lt,
            '!=': operator.ne, "OR": operator.or_, "AND": operator.and_}


class NotSDSRequest(Exception):
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
            raise NotSDSRequest()

    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class BaseSDSHandler(object):
    """
    This is an abstract handler for Proxy/Object Server middleware
    """
    request = _request_instance_property()

    def __init__(self, request, conf, app, logger):
        """
        :param request: swob.Request instance
        :param conf: gatway conf dict
        """
        self.request = request
        self.sds_containers = [conf.get('storlet_container'),
                               conf.get('storlet_dependency')]
        self.app = app
        self.logger = logger
        self.conf = conf

    def _setup_storlet_gateway(self):
        self.storlet_gateway = vsg.SDSGatewayStorlet(
            self.conf, self.logger, self.app, self.api_version,
            self.account, self.container, self.obj, self.request.method)

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
            self.logger.debug('SDS Storlets - Account disabled for storlets')
            return HTTPBadRequest('SDS Storlets -' +
                                  'Account disabled for storlets',
                                  request=self.request)

        return True

    def _call_storlet_gateway_on_put(self, storlet_list):
        """
        Call gateway module to get result of storlet execution
        in PUT flow
        """
        return self.storlet_gateway.execute_storlet(self.request, storlet_list)

    def _call_storlet_gateway_on_get(self, resp, storlet_list):
        """
        Call gateway module to get result of storlet execution
        in GET flow
        """
        raise NotImplementedError()

    def apply_storlet_on_get(self, resp, storlet_list):
        resp = self._call_storlet_gateway_on_get(resp, storlet_list)
        if 'Content-Length' in resp.headers:
            resp.headers.pop('Content-Length')
        if 'Transfer-Encoding' in resp.headers:
            resp.headers.pop('Transfer-Encoding')
        return resp

    def apply_storlet_on_put(self, storlet_list):
        self.request = self._call_storlet_gateway_on_put(storlet_list)

        if 'Storlet-Executed' in self.request.headers:
            self.request.headers.pop('Storlet-Executed')

        if 'CONTENT_LENGTH' in self.request.environ:
            self.request.environ.pop('CONTENT_LENGTH')
        self.request.headers['Transfer-Encoding'] = 'chunked'


class SDSProxyHandler(BaseSDSHandler):

    def __init__(self, request, conf, app, logger):
        super(SDSProxyHandler, self).__init__(
            request, conf, app, logger)

        self.redis_connection = redis.StrictRedis(
            host='controller', port=6379, db=0)

        # Dynamic binding of policies
        self.account_key_list = self.redis_connection.keys(
            "pipeline:" + str(self.account) + "*")

        self.storlet_data = None
        key = self.account + "/" + self.container + "/" + self.obj
        for target in range(3):
            self.target_key = key.rsplit("/", target)[0]
            if 'pipeline:' + self.target_key in self.account_key_list:
                self.storlet_data = self.redis_connection.lrange(
                    'pipeline:' + self.target_key, 0, -1)
                break

        self.storlet_data = self.storlet_data[::-1]

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

    def check_size_type(self, storlet_metadata):

        correct_type = True
        correct_size = True

        if 'object_type' in storlet_metadata:
            obj_type = storlet_metadata['object_type']
            correct_type = self._get_object_type() in \
                self.redis_connection.lrange(obj_type, 0, -1)

        if 'object_size' in storlet_metadata:
            object_size = storlet_metadata['object_size'].replace("'", "\"")
            object_size = json.loads(object_size)

            op = mappings[object_size[0]]
            obj_lenght = int(object_size[1])

            correct_size = op(
                int(self.request.headers['Content-Length']), obj_lenght)

        return correct_type and correct_size

    @property
    def is_sds_object_put(self):
        return (self.container in self.sds_containers and self.obj and
                self.request.method == 'PUT')

    def handle_request(self):
        if self.is_sds_object_put:
            return self.request.get_response(self.app)
        else:
            if hasattr(self, self.request.method):
                resp = getattr(self, self.request.method)()
                return resp
            else:
                return self.request.get_response(self.app)

    def _build_storlet_list(self):
        storlet_list = {}
        for storlet in self.storlet_data:
            stor_acc_metadata = self.redis_connection.hgetall(
                str(self.target_key) + ":" + str(storlet))

            storlet_metadata = self.redis_connection.hgetall(
                "storlet:" + stor_acc_metadata["storlet_id"])

            if storlet_metadata["is_" + self.request.method.lower()] == "True":
                if self.check_size_type(stor_acc_metadata):
                    if "execution_server" in stor_acc_metadata.keys():
                        server = stor_acc_metadata["execution_server"]
                    else:
                        server = storlet_metadata["execution_server"]

                    storlet_execution = {'storlet': storlet,
                                         'params': stor_acc_metadata["params"],
                                         'server': server}
                    launch_key = len(storlet_list.keys())
                    storlet_list[launch_key] = storlet_execution

        return storlet_list

    def _call_storlet_gateway_on_get(self, resp, storlet_list):
        resp = self.storlet_gateway.execute_storlet(resp, storlet_list)
        resp.headers.pop('Vertigo')
        resp.headers.pop("Storlet-Executed")
        return resp

    def GET(self):
        """
        GET handler on Proxy
        """
        if self.storlet_data:
            self.app.logger.info('SDS Storlets - ' + str(self.storlet_data))
            storlet_list = self._build_storlet_list()
            self.request.headers['Storlet-List'] = json.dumps(storlet_list)

        original_resp = self.request.get_response(self.app)

        if 'Vertigo' in original_resp.headers and \
                self.is_account_storlet_enabled():
            self.logger.info(
                'SDS Storlets - There are Storlets to execute '
                'from object server')
            self._setup_storlet_gateway()
            storlet_list = json.loads(original_resp.headers['Vertigo'])
            return self.apply_storlet_on_get(original_resp, storlet_list)

        # NO STORTLETS TO EXECUTE ON PROXY
        elif 'Storlet-Executed' in original_resp.headers:
            if 'Transfer-Encoding' in original_resp.headers:
                original_resp.headers.pop('Transfer-Encoding')
            original_resp.headers['Content-Length'] = None

        return original_resp

    def PUT(self):
        """
        PUT handler on Proxy
        """

        if self.storlet_data:
            self.app.logger.info('SDS Storlets - ' + str(self.storlet_data))
            storlet_list = self._build_storlet_list()
            self.request.headers['Storlet-List'] = json.dumps(storlet_list)
            self.request.headers[
                'Original-Size'] = self.request.headers['Content-Length']
            if 'ETag' in self.request.headers:
                self.request.headers[
                    'Original-Etag'] = self.request.headers['ETag']
                # The object goes to be modified by some Storlet, so we
                # delete the Etag from request headers to prevent checksum
                # verification.
                self.request.headers.pop('Etag')
            else:
                self.request.headers['Original-Etag'] = ""

            if storlet_list and self.is_account_storlet_enabled():
                self._setup_storlet_gateway()
                self.apply_storlet_on_put(storlet_list)
            else:
                self.logger.info(
                    'SDS Storlets - Account disabled for Storlets')
        else:
            self.logger.info('SDS Storlets - No Storlets to execute')

        return self.request.get_response(self.app)


class SDSObjectHandler(BaseSDSHandler):

    def __init__(self, request, conf, app, logger):
        super(SDSObjectHandler, self).__init__(
            request, conf, app, logger)

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

    def _augment_storlet_list(self, storlet_list):
        new_storlet_list = {}

        # REVERSE EXECUTION
        if storlet_list:
            for key in reversed(sorted(storlet_list)):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = storlet_list[key]

        # Get storlet list to execute from proxy
        if 'Storlet-List' in self.request.headers:
            req_storlet_list = json.loads(self.request.headers['Storlet-List'])

            for key in sorted(req_storlet_list):
                launch_key = len(new_storlet_list.keys())
                new_storlet_list[launch_key] = req_storlet_list[key]

        return new_storlet_list

    def _set_iostack_metadata(self):
        iostack_metadata = {}
        storlet_executed_list = json.loads(
            self.request.headers['Storlet-List'])
        iostack_metadata[
            "original-etag"] = self.request.headers['Original-Etag']
        iostack_metadata[
            "original-size"] = self.request.headers['Original-Size']
        iostack_metadata["storlet-list"] = storlet_executed_list

        return iostack_metadata

    def _call_storlet_gateway_on_get(self, resp, storlet_list):
        resp = self.storlet_gateway.execute_storlet(resp, storlet_list)
        return resp

    def GET(self):
        """
        GET handler on Object
        """

        """
        If orig_resp is GET we will need to:
        - Take the object metadata info
        - Execute the storlets described in the metadata info
        - Execute the storlets described in redis
        - Return the result
        """

        original_resp = self.request.get_response(self.app)

        iostack_metadata = sc.get_metadata(original_resp)
        if iostack_metadata:
            original_resp.headers["ETag"] = iostack_metadata["original-etag"]
            original_resp.headers[
                "Content-Length"] = iostack_metadata["original-size"]
            storlet_list = self._augment_storlet_list(
                iostack_metadata["storlet-list"])

            if self.is_account_storlet_enabled():
                self._setup_storlet_gateway()
                return self.apply_storlet_on_get(original_resp, storlet_list)
            else:
                self.logger.info(
                    'SDS Storlets - Account disabled for Storlets')

        return original_resp

    def PUT(self):
        """
        PUT handler on Object
        """
        if 'Vertigo' in self.request.headers and \
                self.is_account_storlet_enabled():
            self.logger.info('SDS Storlets - There are Storlets to execute')
            self._setup_storlet_gateway()
            storlet_list = json.loads(self.request.headers['Vertigo'])
            self.apply_storlet_on_put(storlet_list)

        original_resp = self.request.get_response(self.app)

        if 'Storlet-List' in self.request.headers:
            iostack_metadata = self._set_iostack_metadata()
            if not sc.put_metadata(self.request, iostack_metadata, self.app):
                self.app.logger.error('SDS Storlets - ERROR: Error writing'
                                      'metadata in an object')
                # TODO: Rise exception writting metadata
            original_resp.headers['ETag'] = iostack_metadata['original-etag']

        return original_resp


class SDSHandlerMiddleware(object):

    def __init__(self, app, conf, sds_conf):
        self.app = app
        self.exec_server = sds_conf.get('execution_server')
        self.logger = get_logger(conf, log_route='sds_handler')
        self.sds_conf = sds_conf
        self.containers = [sds_conf.get('storlet_container'),
                           sds_conf.get('storlet_dependency')]
        self.handler_class = self._get_handler(self.exec_server)

    def _get_handler(self, exec_server):
        if exec_server == 'proxy':
            return SDSProxyHandler
        elif exec_server == 'object':
            return SDSObjectHandler
        else:
            raise ValueError(
                'configuration error: execution_server must be either proxy'
                ' or object but is %s' % exec_server)

    @wsgify
    def __call__(self, req):
        try:
            request_handler = self.handler_class(
                req, self.sds_conf, self.app, self.logger)
            self.logger.debug('sds_handler call in %s: with %s/%s/%s' %
                              (self.exec_server, request_handler.account,
                               request_handler.container,
                               request_handler.obj))
        except HTTPException:
            raise
        except NotSDSRequest:
            return req.get_response(self.app)

        try:

            # start = time.time()
            response = request_handler.handle_request()
            # end = time.time() - start

            # self.logger.exception('SDS Storlets - Execution Time: '+str(end))

            return response
            # return request_handler.handle_request()

        except HTTPException:
            self.logger.exception('SDS Storlets execution failed')
            raise
        except Exception:
            self.logger.exception('SDS Storlets execution failed')
            raise HTTPInternalServerError(body='SDS Storlets execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    sds_conf = dict()
    sds_conf['execution_server'] = conf.get('execution_server', 'object')

    sds_conf['storlet_timeout'] = conf.get('storlet_timeout', 40)
    sds_conf['storlet_container'] = conf.get('storlet_container',
                                             'storlet')
    sds_conf['storlet_dependency'] = conf.get('storlet_dependency',
                                              'dependency')
    sds_conf['reseller_prefix'] = conf.get('reseller_prefix', 'AUTH')

    configParser = ConfigParser.RawConfigParser()
    configParser.read(conf.get('storlet_gateway_conf',
                               '/etc/swift/storlet_docker_gateway.conf'))

    additional_items = configParser.items("DEFAULT")
    for key, val in additional_items:
        sds_conf[key] = val

    def swift_sds(app):
        return SDSHandlerMiddleware(app, conf, sds_conf)

    return swift_sds
