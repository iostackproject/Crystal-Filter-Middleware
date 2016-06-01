import crystal_filter_storlet_gateway as storlet_gateway
from swift.common.swob import Request
import json

class Singleton:
    """
    A non-thread-safe helper class to ease implementing singletons.
    This should be used as a decorator -- not a metaclass -- to the
    class that should be a singleton.

    The decorated class can define one `__init__` function that
    takes only the `self` argument. Other than that, there are
    no restrictions that apply to the decorated class.

    To get the singleton instance, use the `Instance` method. Trying
    to use `__call__` will result in a `TypeError` being raised.

    Limitations: The decorated class cannot be inherited from.
    """

    def __init__(self, decorated):
        self._decorated = decorated

    def Instance(self, **args):
        """
        Returns the singleton instance. Upon its first call, it creates a
        new instance of the decorated class and calls its `__init__` method.
        On all subsequent calls, the already created instance is returned.

        """
        logger = args['log']
        try:
            if self._instance:
                logger.info("Crystal - Singleton instance of filter"
                            " control already created")
                return self._instance
        except AttributeError:
            logger.info("Crystal - Creating singleton instance of"
                        " filter control")
            self._instance = self._decorated(**args)
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)


@Singleton
class CrystalFilterControl():
    def __init__(self, conf, log):
        self.logger = log
        self.conf = conf
        self.server = self.conf.get('execution_server')


    def _setup_storlet_gateway(self, conf, logger, app, api_version, 
                               account, container, obj, method):
 
        return storlet_gateway.SDSGatewayStorlet(conf, logger, app, 
                                                api_version, account, 
                                                container, obj, method)
            
    def execute_filters(self, req_resp, filter_exec_list, conf, logger, app,
                        api_version, account, container, obj, method):
        
        on_other_server = dict()
        filter_executed = False
        storlet_gateway = None
        app_iter = None
        
        for key in sorted(filter_exec_list):
            filter_data = filter_exec_list[key]            
            server = filter_data["execution_server"]

            if server == self.server:
                
                if filter_data['type'] == 'storlet':
                    if not storlet_gateway:
                        storlet_gateway = self._setup_storlet_gateway(conf, logger, app,
                                                                  api_version,account,
                                                                  container, obj, method)
                
                    app_iter = storlet_gateway.execute_storlet(req_resp, 
                                                               filter_data,
                                                               app_iter)
                    filter_executed = True
                    
                else:
                    # TODO: Native Filters
                    pass
   
            else:
                on_other_server[key] = filter_exec_list[key]
              
        if on_other_server:
            req_resp.headers['CRYSTAL-FILTERS'] = json.dumps(on_other_server)
        
        if filter_executed:
            if isinstance(req_resp, Request):
                req_resp.environ['wsgi.input'] = app_iter
            else:
                req_resp.app_iter = app_iter
        
        return req_resp
