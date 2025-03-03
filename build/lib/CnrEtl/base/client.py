import csv
import hashlib
import json
import pathlib
import re
from dict_recursive_update import recursive_update
from jmespath import search as jpath
from ..misc import *
from glob import glob as listFiles
import shopify
from typing import List
import collections
from dict_recursive_update import recursive_update
from  slugify import slugify
import argparse
import click
from optparse import OptionParser


class BaseRecord:
    def __init__(self,recordId,data,type="None"):
        self.data = self.prune(data,type)
  
        for key,value in self.data.items():
            setattr(self,key,value)
            
        
        
        self.recordId = recordId
        self.type = type
        
        if self.data is None:
            return None    
    def has(self,key):
        return hasattr(self,key) or key in self.data
    def ignoreColumns(self,type):
        return []
    
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
        
    @staticmethod
    def load(self,recordId,type="None"):
        data = {}
        return BaseRecord(recordId,data,type)
        
        
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
    @staticmethod
    def exists(self,recordId,type):
        return False
    def rm(self):
        pass
    def getExternalId(self):
        
        return None
            
    def write(self,data=None):
        pass
    def reload(self):
        pass
    
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
    @staticmethod
    def list(self,type):
        return []
    
    
    
    
class ConsolidatedRecord(BaseRecord):
    pass
    

class BaseClient():
    def __init__(self,**kwargs):
        for k,v in kwargs.items():
            
            setattr(self,k,v)
        
        
        if hasattr(self,"selector") and getattr(self,"selector") is not None:
           setattr(self,"only",getattr(self,self.selector)())
        if hasattr(self,"param"):
            self.params = {}
            for param in self.params:
                parts = list(map(lambda x:x.strip(),param.split("=")))
                key =  parts[0]
                val = parts[1]
                self.params[key] = val.split(",") if "," in val else val
        
            
        if kwargs.get("configObject"):
            self.configObject = kwargs.get("configObject")
        else:
            if not pathlib.Path("config.json").exists():
                raise FileNotFoundError(f"Pass either configObject= or place config.json in the directory with the following fields:\n\n{self.configDefault}")
            else:
                self.configObject = json.load(open("config.json"))
        
        self.mappings = {}
        for file in listFiles("mappings/*.csv"):
            mappingType = file.split("/")[-1].split(".")[0]
            self.mappings[mappingType] = {}
            reader = csv.DictReader(open(file),delimiter=',',quotechar='"')
            for row in reader:
                self.mappings[mappingType][row.get("source")] = MappingItem(row)            
            
        pass
    def params(self,key,default=None):
        data = self.params
        return default if not data else data
    
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
                if not field.startswith("_"):
                    ret[map[field] if field in map else field] = address[field]
        return ret
    
class RecordAwareClient(BaseClient):
    def loadRecord(self,id,type="inventoryItem",prune=True,searchable=False):
        pass
    def recordExists(self,id,type):
        pass
    
    def consolidatedRecordExists(self,id,type):
        pass
    def writeRecord(self,id,data,type):
        pass
    def loadConsolidateRecord(self,id,type="product",searchable=False):
        pass
    def writeConsolidatedRecord(self,id,data,type):
        pass
    def recordList(self,type):
        pass
    def consolidatedRecordList(self,type) -> List[BaseRecord]:
        pass
    def consolidatedRecordIds(self,type) -> List[BaseRecord]:
        pass
    
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