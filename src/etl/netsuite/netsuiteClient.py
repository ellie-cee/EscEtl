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

class NetSuiteRecord:
    def __init__(self,file,dataProcessor=lambda x:x,blank=False):
        if blank:
            self.data = {}
            self.filename = None
            self.recordId = None
            self.blank = True
        else:
            self.filename = file
            try:
                self.data = dataProcessor(json.load(open(file)))
            except:
                pathlib.Path(file).unlink()
                return None
                
                traceback.print_exc()
                sys.exit()
                self.data = {}
            self.blank = False
            for key,value in self.data.items():
                setattr(self,key,value)
            self.recordId = file.split("/")[-1].split(".")[0].split("-")[-1]
            
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
        json.dump(data if data is not None else self.data,open(self.filename,"w"),indent=1)
    def reload(self):
        self.data = json.load(open(self.filename))
    def dump(self,printIt=False):
        if (printIt):
            print(json.dumps(self.data,indent=1))
        else:
            return self.data
        
        
class NetSuiteClient:
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
        for k,v in kwargs.items():
            setattr(self,k,v)
            
        if kwargs.get("configObject"):
            self.configObject = kwargs.get("configObject")
        else:
            if not pathlib.Path("config.json").exists():
                raise FileNotFoundError(f"Pass either configObject= or place config.json in the directory with the following fields:\n\n{self.configDefault}")
            else:
                self.configObject = json.load(open("config.json"))
        #self.ns = self.netsuiteInit()
        for path in [
                "records",
                "output",
                "records/inventoryItem",
                "records/customer",
                "records/salesOrder",
                "records/consolidated/products",
                "records/consolidated/companies"
            ]:
            if not pathlib.Path(path).exists():
                pathlib.Path(path).mkdir()
        self.mappings = {}
        for file in listFiles("mappings/*.csv"):
            mappingType = file.split("/")[-1].split(".")[0]
            self.mappings[mappingType] = {}
            reader = csv.DictReader(open(file),delimiter=',',quotechar='"')
            for row in reader:
                self.mappings[mappingType][row.get("source")] = MappingItem(row)            
            
        pass
    def has(self,k):
        return hasattr(self,k)
    def get(self,k,default=None):
        if self.has(k):
            return getattr(self,k)
        return default
    
    def shopifyInit(self):
        if self.config("shopifyToken") is not None:
            shopify.ShopifyResource.activate_session(
                shopify.Session(
                    f"{self.config('shopifySite')}/admin",
                    self.config('shopifyApiVersion'),
                    self.config('shopifyToken')
                )
            )
    def mapping(self,type):
        if type in self.mappings:
            return self.mappings[type]
        return None
    def map(self,mapType,key,default=None):
        mr = self.mappings.get(mapType,{}).get(key)
        if mr is not None:
            if hasattr(mr,"oto"):
                return mr.oto
            else:
                return mr
        else:
            return default
        
    def config(self,key,default=None):
        return self.configObject.get(key,default)
        
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
        filename = f"records/{type}/{id}.json"
        if pathlib.Path(filename).exists():
            return NetSuiteRecord(filename,dataProcessor=lambda x:self.prune(x,type))
        else:
            return None
    def recordExists(self,id,type):
        filename = f"records/{type}/{id}.json"
        return pathlib.Path(filename).exists()
    def consolidatedRecordExists(self,id,type):
        filename = f"records/consolidated/{type}/{type}-{id}.json"
        return pathlib.Path(filename).exists()
    def writeRecord(self,id,data,type):
        if not pathlib.Path(f"records/{type}"):
            pathlib.Path(f"records/{type}").mkdir()
        json.dump(data,open(f"records/{type}/{id}.json","w"),indent=1)
            
    def loadConsolidateRecord(self,id,type="product",searchable=False) -> NetSuiteRecord:
        filename = f"records/consolidated/{type}/{type}-{id}.json"
        if pathlib.Path(filename).exists():
            return NetSuiteRecord(filename)
        else:
            return None
    def writeConsolidatedRecord(self,id,data,type):
        if not pathlib.Path(f"records/consolidated/{type}"):
            pathlib.Path(f"records/consolidated/{type}").mkdir()
        json.dump(data,open(f"records/consolidated/{type}/{type}-{id}.json","w"),indent=1)
        
        
    def recordList(self,type):
        return list(
            map(
                lambda x: x.split("/")[-1].replace(".json",""),
                listFiles(f"records/{type}/*.json")
            )
        )
    def consolidatedRecordList(self,type) -> List[NetSuiteRecord]:
        return list(
            map(
                lambda x:NetSuiteRecord(x),
                listFiles(f"records/consolidated/{type}/{type}-*.json")
            )
        )
        
    def hashOf(self,data):
        return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()
    
    def deduplicate(self,dest,reference):
        return {key:dest[key] for key in filter(lambda x:self.hashOf(dest.get(x,{}))!=self.hashOf(reference.get(x,{})),dest.keys())}
    
    def ignoreColumns(self,recordType):
        return ["links","autoReorderPoint", "froogleProductFeed", "incomeAccount", "manufacturerState", "matchBillToReceipt", "multManufactureAddr", "nexTagProductFeed", "shoppingProductFeed", "shopzillaProductFeed", "subsidiary", "supplyReplenishmentMethod", "trackLandedCost", "transferPriceUnits", "translations", "unitsType", "useBins", "useMarginalRates", "yahooProductFeed"],
    
    def ignoreVariantColumns(self):
        return [
            
        ]    
    
    def prune(self,record,type,alsoIgnore=[]):
        ret = {"customFields":{}}
        for key in filter(lambda x: x not in self.ignoreColumns(type) and x not in alsoIgnore,record.keys()):
            if key.startswith("cust") and key!="customFields":
                if record[key] is not None:
                    ret["customFields"]["_".join(key.split("_")[1:]) if "_" in key else key] = record[key]
            else:
                ret[key] = record[key]
            
        return self.walk(ret)
    def globalIgnore(self):
        return ["count","offset","hasMore","links","totalResult"]
    def walk(self,value):
        if type(value) is dict:
            intermediate = {x:value[x] for x in filter(lambda x: x not in self.globalIgnore(),list(value.keys()))}
            if "items" in intermediate:
                if "urlFragment" in intermediate:
                    return {x:self.walk(intermediate[x]) for x in intermediate.keys()}
                    #intermediate["items"] = self.walk(intermediate["items"]) if len(intermediate["items"])>0 else None
                else:
                    return self.walk(intermediate["items"]) if len(intermediate["items"])>0 else None
            elif len(list(intermediate.keys()))<1:
                return None
            else:
                return {x:self.walk(intermediate[x]) for x in intermediate.keys()}
        elif type(value) is list:
            return [self.walk(x) for x in value]
        else:
            return value
    def privatize(self,value):
        if type(value) is dict:
            return {x:self.privatize(value[x]) for x in filter(lambda y:not y.startswith("_"),list(value.keys()))}
        elif type(value) is list:
            return [self.privatize(x) for x in value]
        else:
            return value
        
    def setArgs(self,**kwargs):
        for k,v in kwargs.items():
            setattr(self,k,v)
            
class AddressAwareClient:
    def addressByType(self,addresses,key):
        for address in addresses:
            if address.get(key):
                return address
        return None

    def justAddrFields(self,address):
        ret = {}    
        for field in ["address1","address2","city","countryCode","recipient","zip","zoneCode","phone"]:
            if address.get(field,"")=="" and field in ["phone"]:
                continue
            ret[field] = address.get(field)
            
        return ret
    def meetsAddressMinimum(self,address):
        ret = True
        for field in ["address1","city","zip","zoneCode"]:
            if address.get(field,"") is None or address.get(field,"")=="":
                ret = False          
        return ret
    def addressHandle(self,address):
        try:
            return  slugify(" ".join([address.get(x,"") if address.get(x) is not None else "" for x in ["address1","city","state","zoneCode","countryCode"]]))
        except:
            
            
            sys.exit()
    
    def ignoreRecipients(self):
        return []
    def isIgnoredRecipient(self,recipient):
        for ignored in self.ignoreRecipients():
            if recipient.startswith(ignored):
                return True
        return False
    def parseAddressFromText(self,line):
        ret = {}
        def isCSZ(part):
            for subpart in part.split(" "):
                
                if subpart.upper() in can_province_codes or subpart.upper() in us_state_codes:
                    return True
            return False
                    
        def extract_part(parts,evaluator,transformer=lambda x:x):
            value = None
            excludeValue = None
            for part in parts:
                if evaluator(part):
                    excludeValue = part
                    value = transformer(part)
            if value is not None:
                parts = list(filter(lambda x: x!=excludeValue,parts))
            return value,parts
        splitter = "\n"
        if "<br>" in line:
            splitter = "<br>"
        parts = list(filter(lambda x: x is not None,reversed(line.replace("\r","").split(splitter))))
        label1 = None
        label2 = None
        recipient = None
        if re.match(r'[0-9]+',parts[0]) is None:
            recipient = parts.pop()
        address,parts = extract_part(parts,lambda x:re.match(r'[0-9]+.*',x) is not None and re.search(r'[a-zA-Z]+',x) is not None)
        
        phone,parts = extract_part(parts,lambda x:is_phone(x),lambda x:format_phone(x))
        csz,parts = extract_part(parts,lambda x:isCSZ(x),lambda x:x.split(" "))
        country,parts = extract_part(parts,lambda x:country_code(x) is not None,lambda x:country_code(x))
        
        if country is None:
            country = "US"
        for field in [address,csz]:
            if field is None:
                return None
        
        zip = None
        zoneCode = None
        city = None
        isCanada = country=="CA"
        if not isCanada:
            for cszPart in csz:
                if cszPart in can_province_codes:
                    isCanada = True
                    
        
        if isCanada:
            
            
            zip = " ".join(list(reversed([csz.pop(),csz.pop()])))
            
            zoneCode = csz.pop()
            city = " ".join(csz)
        else:
            zip = csz.pop()
            zoneCode = csz.pop()
            city = " ".join(csz)
                
        ret = {
            "address1":address,
            "recipient":recipient,
            "countryCode":country,
            "zip":zip,
            "zoneCode":zoneCode,
            "city":city
        }
        if isCanada:
            ret["countryCode"]="CA"
            
        if ret["countryCode"] in us_state_codes:
            ret["zoneCode"] = ret["countryCode"]
            ret["countryCode"] = "US"
        if ret["countryCode"] in can_province_codes:
            ret["zoneCode"] = ret["countryCode"]
            ret["countryCode"] = "CA"
            
        if phone is not None:
            ret["phone"] = phone
               
        return ret        
    def mapAddress(self,rawAddress,remap=True):
        if rawAddress is None:
            print("no address!!!!")
            return rawAddress
        
        recipient = rawAddress.get("attention",rawAddress.get("addressee",""))
        if self.isIgnoredRecipient(recipient):
                print(f"ignored recipient {recipient}")
                return None
        details = {
                "taxExemptions":[],
                "_externalId":rawAddress.get("id"),
                "address1":rawAddress.get("addr1"),
                "address2":rawAddress.get("addr2",""),
                "city":rawAddress.get("city"),
                "countryCode":rawAddress.get("country",{}).get("id"),
                "recipient":recipient,
                "zoneCode":rawAddress.get("state"),
                "zip":rawAddress.get("zip"),
            }
        if is_phone(rawAddress.get("addrPhone","")):
            details["phone"] = format_phone(rawAddress.get("addrPhone"))
                
            
                
        if rawAddress.get("addr1") is None or rawAddress.get("addr1")=="":
            parsed =  self.parseAddressFromText(rawAddress.get("addrText"))
            if parsed is None:
                    print(f"unable to parse address string: {rawAddress.get('addrText','N/A')}")
                    # worng adderss
                    # I'm just conna let that stand becuase I think it's funny
                    return None
            if parsed.get("recipient") is None:
                parsed["recipient"] = parsed.get("address1")
                    
            
            for k,v in parsed.items():
                    details[k] = v
            if not self.meetsAddressMinimum(details):
                print("address does not meet minimum requirements")
                return None
            
        
        details = fixAddress(details)
        #return self.remapAddress(details) if remap else details
        return details
    def remapAddress(self,address):
        map = {"zoneCode":"provinceCode"}
        ignore = ["recipient","taxExemptions","externalId"]
        ret = {}
        for field in address.keys():
            if field not in ignore:
                
                ret[map[field] if field in map else field] = address[field]
        return ret
    
class RecordAwareClient(NetSuiteClient):
    defaultRecordType = "product"
    defaultNetsuiteRecordType = "inventoryItem"
    def getNetsuiteRecordType(self,**kwargs):
        return kwargs.get("forceType") if "forceType" in kwargs else self.defaultNetsuiteRecordType
    def getRecordType(self,**kwargs):
        return kwargs.get("forceType") if "forceType" in kwargs else self.defaultRecordType
    def recordList(self):
        return super().recordList(self.defaultNetsuiteRecordType)
    def supportingRecordList(self,type):
        return super().recordList(type)
    def consolidatedRecordList(self) -> List[NetSuiteRecord]:
        return super().consolidatedRecordList(self.defaultRecordType)
    def loadRecord(self,id,**kwargs) -> NetSuiteRecord:
        return super().loadRecord(id,self.getNetsuiteRecordType(**kwargs))
    def loadSupportingRecord(self,id,type,**kwargs) -> NetSuiteRecord:
        return super().loadRecord(id,type,**kwargs)
    def loadConsolidateRecord(self, id,**kwargs) -> NetSuiteRecord:
        rec = super().loadConsolidateRecord(id, self.getRecordType(**kwargs))
        if rec is not None:
            return rec
    def loadSupportingConsolidateRecord(self, id,type) -> NetSuiteRecord:
        rec = super().loadConsolidateRecord(id, type)
        if rec is not None:
            return rec
    def writeRecord(self, id, data):
        return super().writeRecord(id, data, self.defaultNetsuiteRecordType)
    def writeConsolidatedRecord(self, id, data,**kwargs):
        return super().writeConsolidatedRecord(id, data, self.getRecordType(**kwargs))
    def recordType(self,recordId):
        return self.defaultNetsuiteRecordType
    
class CustomerRecordAwareClient(RecordAwareClient):
    defaultRecordType = "company"
    defaultNetsuiteRecordType = "customer"
    def loadConsolidateRecord(self, id,**kwargs) -> NetSuiteRecord:
        for recordType in ["company","customer"]:
            if self.consolidatedRecordExists(id,recordType):
               return super().loadConsolidateRecord(id,type=recordType)
        return None
        
class OrderRecordAwareClient(RecordAwareClient):
    defaultNetsuiteRecordType = "salesOrder"
    defaultRecordType = "order"
    
class ProductRecordAwareClient(RecordAwareClient):
    defaultRecordType = "product"
    defaultNetsuiteRecordType = "inventoryItem"
    def loadRecord(self,id) -> NetSuiteRecord:
        for type in ["serviceSaleItem","inventoryItem","assemblyItem"]:
            if self.recordExists(id,type):
                return super().loadRecord(id,forceType=type)
                
        return None
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
    

