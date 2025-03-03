import os
import re
import sys
import asyncio
import csv
import json
import traceback
from netsuite import NetSuite, Config, TokenAuth
import pathlib
from jmespath import search as jpath
from glob import glob as listFiles
import hashlib
import shopify
import pathlib
from ..misc import *
from typing import List
import collections
from dict_recursive_update import recursive_update
from  slugify import slugify
import argparse
import click
from optparse import OptionParser
from ..base import *

class NetSuiteRecord(BaseRecord):
    def __init__(self,recordId,data,type="inventoryItem"):
        super().__init__(recordId.strip(),data,type)
        
        
    def getExternalId(self):
        for field in ["externalId","netSuiteId","id"]:
            if field in self.data:
                return self.data.get(field)
        return None
    def filename(self):
        return f"records/{self.type}/{self.recordId}.json"
    def filepath(self):
        return f"records/{self.type}"
    
    @staticmethod
    def load(recordId,type="inventoryItem"):
        
        try:
            data = json.load(open(f"records/{type}/{recordId.strip()}.json"))
            return NetSuiteRecord(recordId,data,type)
        except:
            return None
    def write(self):
        if not pathlib.Path(self.filepath()):
            pathlib.Path(self.filepath()).mkdir()
        json.dump(self.jsonify(self.data),open(self.filename(),"w"),indent=1)
            
    @staticmethod
    def list(type):
        return listFiles(f"records/{type}/*.json")
            
    @staticmethod
    def exists(recordId,type):
        return pathlib.Path(f"records/{type}/{recordId.strip()}.json").exists()
            
    def reload(self):
        self.data = json.load(open(self.filename()))
        
    def jsonify(self,value):
        if isinstance(value,dict):
            ret = {}
            for key,value in value.items():
                if isinstance(value,NetSuiteRecord):
                    ret[key] = self.jsonify(value.data)
                else:
                    ret[key] = self.jsonify(value)
            return ret
        elif isinstance(value,list):
            return [self.jsonify(x) for x in value]
        elif isinstance(value,NetSuiteRecord):
            return value.data
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
                if isinstance(value,NetSuiteRecord):
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
class NetSuiteConsolidatedRecord(NetSuiteRecord):
    def rm(self):
        try:
            pathlib.Path(self.filename).unlink()
        except:
            pass
    def write(self):
        return super().write()
    def filename(self):
        return f"records/consolidated/{self.type}/{self.type}-{self.recordId.strip()}.json"
    def filepath(self):
        return f"records/consolidated/{self.type}"
    @staticmethod
    def exists(recordId,type):
        
        return pathlib.Path(f"records/consolidated/{type}/{type}-{recordId.strip()}.json").exists()
    
    @staticmethod
    def load(recordId,type="inventoryItem"):
        if not NetSuiteConsolidatedRecord.exists(recordId,type):
            return None
        try:
            
            data = json.load(open(f"records/consolidated/{type}/{type}-{recordId.strip()}.json"))
            return NetSuiteConsolidatedRecord(recordId,data,type)
        except:
            print(f"records/consolidated/{type}/{type}-{recordId.strip()}.json")
            return None
    @staticmethod
    def list(type):
        return list(
            map(
                lambda x:x.split("/")[-1].split(".")[0].split("-")[-1],
                listFiles(f"records/consolidated/{type}/{type}-*.json")
            )
        )
    
            
    
class NetSuiteClient(BaseClient):
    fieldMap = {
        "itemId":"sku",
    }
    configDefault = """
    NetSuite Fields:
    
    "appId":"",
    "consumerKey":"",
    "consumerSecret":fdff9b5146e9af45f9e57cd52bef4ef900110ae7de642e66b5b69fc4c546b669",
    "tokenId":"",
    "tokenSecret":"",
    "accountId":"",
    Shopify Fields:
    
    "shopifyToken":"",
    "shopifySite":"<site>.myshopify.com",
    "shopifyApiVersion":"2024-07"
    """
    def __init__(self,**kwargs):
       super().__init__(**kwargs)
       
       self.netsuiteInit()
        
    def netsuiteInit(self):
        return NetSuite(Config(
            account=self.config("accountId"),
            auth=TokenAuth(
                consumer_key=self.config("consumerKey"),
                consumer_secret=self.config("consumerSecret"),
                token_id=self.config("tokenId"),
                token_secret=self.config("tokenSecret"),
            )
        ))
        
    def loadRecord(self,id,type="inventoryItem",prune=True,searchable=False) -> NetSuiteRecord:
        return NetSuiteRecord.load(id,type)
    def recordExists(self,id,type):
        return NetSuiteRecord.exists(id,type)
    def consolidatedRecordExists(self,id,type):
        return NetSuiteConsolidatedRecord.exists(id,type)
        
    def writeRecord(self,id,data,type):
        NetSuiteRecord(id,data,type).write()
    def loadConsolidateRecord(self,id,type="product",searchable=False) -> NetSuiteConsolidatedRecord:
        return NetSuiteConsolidatedRecord.load(id,type)
    def writeConsolidatedRecord(self,id,data,type):
        NetSuiteConsolidatedRecord(id,data,type).write()
    def recordList(self,type):
        return NetSuiteRecord.list(type)
    def consolidatedRecordList(self,type) -> List[NetSuiteRecord]:
        return list(
            map(
                lambda x:NetSuiteConsolidatedRecord.load(x,type),
                NetSuiteConsolidatedRecord.list(type)
            )
        )
    def consolidatedRecordIds(self,type) -> List[NetSuiteRecord]:
        return NetSuiteConsolidatedRecord.list(type)
    
    def hashOf(self,data):
        return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()
    
    def setArgs(self,**kwargs):
        for k,v in kwargs.items():
            setattr(self,k,v)
            
class CustomerRecordAwareClient(NetSuiteClient):
    defaultRecordType = "company"
    defaultNetsuiteRecordType = "customer"
    def loadConsolidateRecord(self, id,**kwargs) -> NetSuiteConsolidatedRecord:
        for recordType in ["company","customer"]:
            
            if NetSuiteConsolidatedRecord.exists(id,recordType):
                
                return NetSuiteConsolidatedRecord.load(id,recordType)
        return None
    def consolidatedRecordList(self):
        return super().consolidatedRecordList("company")
    def consolidatedRecordExists(self, id):
        return super().consolidatedRecordExists(id, "company")
    def writeConsolidatedRecord(self, id, data,forceType="company"):
        return super().writeConsolidatedRecord(id, data, forceType)    
    def loadRecord(self,recordId,prune=True,searchable=True):
    
        ret =  super().loadRecord(recordId,"customer",prune=True)
        
        return ret
    def recordList(self):
        return [x.split("/")[-1].split(".")[0] for x in super().recordList("customer")]
    
class OrderRecordAwareClient(NetSuiteClient):
    defaultNetsuiteRecordType = "cashSale"
    defaultRecordType = "order"
    
class ProductRecordAwareClient(NetSuiteClient):
    defaultRecordType = "product"
    defaultNetsuiteRecordType = "inventoryItem"
    def loadRecord(self,id) -> NetSuiteRecord:
        
        for type in ["serviceSaleItem","inventoryItem","assemblyItem"]:
            if NetSuiteRecord.exists(id,type):
                
                return NetSuiteRecord.load(id,type)
                
        return None
    def loadConsolidateRecord(self,id) -> NetSuiteRecord:
        return NetSuiteConsolidatedRecord.load(id,"product")
    def consolidatedRecordList(self):
        return super().consolidatedRecordList("product")
    def consolidatedRecordExists(self, id):
        return super().consolidatedRecordExists(id, "product")
    def writeConsolidatedRecord(self, id, data):
        return super().writeConsolidatedRecord(id, data, "product")
    def recordList(self, recordType=None):
        recordList = []
        types = ["serviceSaleItem","inventoryItem","assemblyItem"]
        if recordType is not None:
            types = [recordType]
        for type in types:
            recordList = recordList + [x.split("/")[-1].split(".")[0] for x in super().recordList(type)]
        return recordList
        
    def recordType(self,recordId):
        for type in ["serviceSaleItem","inventoryItem","assemblyItem"]:
            if self.recordExists(recordId,type):
                return type
        return None
    

        
class MappingItem:
    def __init__(self,data):
        for k in data.keys():
            setattr(self,k,data[k])
        self.data = data
        self.__dict__ = data
    def __dict__(self):
        return self.data
    def get(self,key):
        return self.data.get(key)
    

