import shopify
import json
from ..misc import *
import logging

logger = logging.getLogger(__name__)


    
class GraphQL:
    def __init__(self,debug=False,searchable=True):
        self.debugging = debug
        self.debuggingIndent=1
        self.searchable = searchable
    def debug(self,value=True,level=1):
        self.debugging = value
        self.debuggingIndent = level
        
    def run(self,query,variables={},searchable=False):
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
        print(self.params)
        ret = self.run(self.query,self.params)
        values = [SearchableDict(x) for x in ret.search(f"{self.dataroot}.nodes",[])]
        if ret.search(f"{self.dataroot}.pageInfo.hasNextPage"):
            self.hasNext = True
            self.cursor = ret.search(f"{self.dataroot}.pageInfo.endCursor")
        else:
            self.hasNext = False
        return values
        
        
