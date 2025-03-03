import json
import pathlib
from dict_recursive_update import recursive_update
from jmespath import search as jpath
from .misc import SearchableDict
from pony.orm import *



class BaseRecord:
    def __init__(self,recordId,dataProcessor=lambda x:x,blank=False):
        for key,value in self.data.items():
            setattr(self,key,value)
        self.recordId = recordId.split("/")[-1].split(".")[0].split("-")[-1]
        
    def setRecordId(self,recordId):
        self.recordId = self.loadRecord()
    def has(self,key):
        return hasattr(self,key) or key in self.data
    
    def get(self,key,default=None):
        ret = self.data.get(key,default)
        
        if ret is None:
            return default
        return ret
    def append(self,key,value):
        if key not in self.data:
            self.data[key] = value
        elif type(self.data[key]) is not list:
            self.data[key] = [self.data[key],value]
        else:
            self.data[key].append(value)
    def appendIfNot(self,key,value):
        if key not in self.data:
            self.data[key] = value
        elif type(self.data[key]) is not list:
            self.data[key] = [self.data[key],value]
        else:
            if value not in self.data[key]:
                self.data[key].append(value)
        
    def getAny(self,*args):
        for arg in args:
            if self.get(arg):
                return arg
        return None
    def set(self,key,value):
        paths = list(reversed(key.split(".")))
        if len(paths)>1:
            object = value
            for k in paths:
                object = {k:object}
            self.data = recursive_update(self.data,object)
        else:
            self.data[key] = value            
    def buildDict(self,path,value,object):
        this = path.pop()
        if len(path)<1:
            return value
        else:
            return self.buildDict(path,value,object[this])
        return object        
        
        self.data[key] = value
    def setData(self,data):
        self.data = data
    def delete(self,key):
        if key in self.data:
            del self.data[key]
    def search(self,path,default=None):
        ret = jpath(path,self.data)
        if ret is None:
            return default
        return ret
    def rm(self):
        try:
            pathlib.Path(self.filename).unlink()
        except:
            pass
    def getExternalId(self):
        for field in ["externalId","netSuiteId","id"]:
            if field in self.data:
                return self.data.get(field)
        return None
            
    def write(self,data=None):
        toWrite = None
        if data is not None:
            toWrite = data
        else:
            toWrite = self.data
        if len(list(toWrite.keys()))>0:
            json.dump(self.jsonify(toWrite),open(self.filename,"w"),indent=1)
    def reload(self):
        self.data = json.load(open(self.filename))
    def jsonify(self,value):
        if isinstance(value,dict):
            ret = {}
            for key,value in value.items():
                if isinstance(value,BaseRecord):
                    ret[key] = self.jsonify(value.data)
                else:
                    ret[key] = self.jsonify(value)
            return ret
        elif isinstance(value,list):
            return [self.jsonify(x) for x in value]
        else:
            return value
    def dump(self,printIt=True):
        if (printIt):
            print(json.dumps(self.jsonify(self.data),indent=1))
        else:
            return self.data
        
    def stripShopify(self):
        self.data = self.stripShopifyFields(self.data)
        
    def stripShopifyFields(self,value):
        if isinstance(value,dict):
            ret = {}
            for key,value in value.items():
                if key.startswith("shopify"):
                    continue
                if key=="companyLocationId":
                    continue
                if isinstance(value,BaseRecord):
                    ret[key] = self.stripShopifyFields(value.data)
                else:
                    ret[key] = self.stripShopifyFields(value)
            return ret
        
        elif isinstance(value,list):
            return [self.stripShopifyFields(x) for x in value]
        else:
            return value
    def getAsSearchable(self,key,default={}):
        val = self.get(key)
        if isinstance(val,list):
            return [SearchableDict(x) for x in val]
        if isinstance(val,dict):
            return SearchableDict(val)
        if val is None:
            return SearchableDict({})
        return val

class BaseClient:
    pass