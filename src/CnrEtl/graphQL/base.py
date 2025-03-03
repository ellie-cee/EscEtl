import shopify
import json
from ..misc import *
import logging
import urllib
import time

logger = logging.getLogger(__name__)

def catchNetWorkError(fn):
    def wrapper(self, *args, **kwargs):
        retry = True
        retryCount = 0
        while retry:
            try:
                ret = fn(self,*args, **kwargs)
                retry = False
                return ret
            except urllib.error.HTTPError as e:
                print(f"{e.reason}: retrying")
                time.sleep(1)
    wrapper.__name__ = fn.__name__
    return wrapper
    
class GraphQL:
    def __init__(self,debug=False,searchable=True,):
        self.debugging = debug
        self.debuggingIndent=1
        self.searchable = searchable
    def debug(self,value=True,level=1):
        self.debugging = value
        self.debuggingIndent = level
        
    @catchNetWorkError    
    def run(self,query,variables={},searchable=True):
        ret = json.loads(shopify.GraphQL().execute(query,variables))
        if searchable or self.searchable:
            return GqlReturn(ret)
        return ret
    def iterable(self,query,params,dataroot="data.products"):
        return GraphQlIterable(query,params,dataroot=dataroot)
        pass
        
class GraphQlIterable(GraphQL):
    def __init__(self, query,params,dataroot="data.products"):
        super().__init__(searchable=True,debug=False)
        self.cursor = None
        self.hasNext = True
        self.query = query
        self.params = params
        self.dataroot = dataroot
    def __iter__(self):
        return self
    def __next__(self):
        if not self.hasNext:
            raise StopIteration
        self.params["after"] = self.cursor
        ret = self.run(self.query,self.params)
        
        if ret.get("data") is not None:
            try:
                self.dataroot = f'data.{next(iter(ret.search("data").keys()),None)}'
            except:
                ret.dump()
                sys.exit()
        
        if ret.hasErrors():
            print(json.dumps(ret.errorMessages(),indent=1))
        values = [GqlReturn(x) for x in ret.search(f"{self.dataroot}.nodes",[])]
        if ret.search(f"{self.dataroot}.pageInfo.hasNextPage"):
            self.hasNext = True
            self.cursor = ret.search(f"{self.dataroot}.pageInfo.endCursor")
            
        else:
            self.hasNext = False
        return values
        
        
