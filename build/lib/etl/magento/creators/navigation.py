import json
import subprocess
from ..netsuiteClient import *
from ..consolidators import *
from etl.graphQL import Customer,Companies,MetaField
from etl.misc import *
from shopify_uploader import ShopifyUploader
import urllib.parse

class NavigationCreator(RecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
    def navify(self,value):
        
        if isinstance(value,dict):
            ret = {}
            for key,value in value.items():                
                match key:
                    case "children":
                        ret["items"] = list(self.navify(value).values())                
                    case "urls":
                        ret["url"] = value.get("dest")
                    case "name":
                        ret["title"] = value
                    case _:
                        ret[key] = self.navify(value)
                
            if "url" in ret:
                ret["type"] = "HTTP"
            return ret
        elif isinstance(value,list):
            return [self.navify(x) for x in value]
        else:
            return value
    def run(self):
        pass
        
            