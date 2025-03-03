import requests
from etl.graphQL import *
import json
from ..netsuiteClient import *
from slugify import slugify
from etl.misc import *
from bs4 import BeautifulSoup
import functools

class ProductConsolidator(ProductRecordAwareClient):
    def __init__(self,**kwargs):
        super().__init__(**kwargs)

    def finalMapVariant(self,record):
        
        data = record
        mapping = self.mapping("fieldNames")
        if type(record) is NetSuiteRecord:
            data = record.data
            
        return data
    
    def finalMapProduct(self,record):
        
        data = record
        mapping = self.mapping("fieldNames")
        if type(record) is NetSuiteRecord:
            data = record.data
            
        return data


            
    def handlePrices(self,record):
        pass
    def mapProductInformation(self,record):
        pass
    def handleOptions(self,record,parent):
        pass
    def handleImages(self,record,parent):
        pass
    def handleMetafields(record,parent):
        pass
    def checkForProductById(self,recordId):
        res = requests.head(f"{self.config('sourceSite')}/product/{recordId}")
        return res.status_code==200
        
    def processType(self,recordType):
        parentItems = {}
        # first pass: identify parents
        for recordId in self.recordList(recordType):
            
            existingRecord = self.loadConsolidateRecord(recordId) 
            record = self.loadRecord(recordId)
            
            
            customFields = record.get("customFields",{})
            record.set("productInformationTab",self.mapProductInformation(record))
            record.set("originalRecordType",recordType)
            if (record.get("isOnline")):
                if jpath("matrixType.id",record)=="PARENT":
                    record.set("children",[])
                    parentItems[recordId] = record
        
        # second pass, fill in children
        for recordId in filter(lambda x: x not in parentItems,self.recordList()):
        
            parent:NetSuiteRecord
            record = self.loadRecord(recordId)
                
            if record.get("parent") and record.search("parent.id") in parentItems:
                if record.get("isInactive",True) or "inactive" in record.get("itemId"):
                    print("isInactive",record.recordId)
                    continue
                
                parent = parentItems[record.search("parent.id")]
                for ignoreField in ["assetAccount","atpMethod", "autoLeadTime", "autoPreferredStockLevel", "averageCost", "baseUnit", "binNumber", "class", "cogsAccount", "consumptionUnit", "copyDescription", "costEstimateType", "costEstimateUnits", "costingMethod", "currency", "enforceminqtyinternally",  "futurehorizon",  "isDropShipItem", "isGCoCompliant", "isLotItem", "isSerialItem", "isSpecialOrderItem",  "itemType", "itemVendor", "manufacturer", "matrixType", "offerSupport", "parent", "roundUpAsComponent", "seasonalDemand", "shipIndividually", "stockUnit",  "weightUnits"]:
                    if ignoreField in record.data:
                        del record.data[ignoreField]
                        if record.has(ignoreField):
                            delattr(record,ignoreField)
                record.set("productInformationTab", self.mapProductInformation(record))
                record.set("_isInactive",False)
                record.set("_itemId",record.get("itemId"))
                customFields = record.get("customFields")
                pricing = None
                if record.get("price") is not None:
                    self.handlePrices(record,parent)
                self.handleOptions(record,parent)  
                
                parent.append("children",record)
                
       
        # pass 3 fill in details       
        getOptionValues = lambda x: [y for y in x.get("options",{}).keys()]
        for recordId in parentItems:
            print("filling in",recordId)
            if hasattr(self,"only") and len(self.only)>0 and recordId not in self.only:
                print("skipping",recordId)
                continue       
            existingRecord = self.loadConsolidateRecord(recordId)
            saveParent = True   
            parent = parentItems[recordId]
            allOptions = {}
            
            self.handleImages(parent)
            
            tentativeChildren = [self.finalMapVariant(child) for child in parent.get("children")]
            tentativeChildrenWithPreserved = []
            
            existingChildrenBySKU = {}
            if existingRecord is not None:
                existingChildrenBySKU = {
                    child.get("SKU"):child for child in filter(lambda x:x.get("shopifyId") is not None,existingRecord.get("children",[]))
                }
            
            for child in tentativeChildren:
                childId = int(child.get("netSuiteId"))
               
                sku = child.get("SKU")
                existingChild = existingChildrenBySKU.get(sku)
                if existingChild is not None:
                    
                    if child.get("isOnline")!=True:
                        existingChild["_purge"]=True
                    tentativeChildrenWithPreserved.append(existingChild)
                else:
                    tentativeChildrenWithPreserved.append(child)
                        
                
            
            hasOptions = False
            finalChildren = []
            for child in tentativeChildrenWithPreserved:
                if child.get("options") is not None:
                    if len(child.get("options",{}).keys())>0:
                        hasOptions = True
                        break
            if hasOptions and False:
                
                finalChildren = []
                variantsByOptions = {}
                for child in tentativeChildrenWithPreserved:
                    
                    childId = int(child.get("netSuiteId"))
                    sku = child.get("SKU")
                    existingChild = existingChildrenBySKU.get(sku)
                    
                    
                    options = child.get("options")
                    if hasOptions and len(list(options.keys()))<1:
                        continue
                    signature = "|".join(list(map(lambda k:f"{k}-{options[k]}",sorted(list(options.keys())))))
                    if existingChild is not None:
                        variantsByOptions[signature] = {
                            "id":childId,
                            "child":existingChild
                        }
                        continue
                        
                    if variantsByOptions.get(signature):
                        if childId>variantsByOptions.get(signature).get("id"):
                            variantsByOptions[signature]["child"] = child
                    else:
                        variantsByOptions[signature] = {
                            "id":childId,
                            "child":child
                        }
                    
                    
                parent.set("children",[cbo.get("child") for cbo in variantsByOptions.values()])
            else:
                finalChildren = tentativeChildrenWithPreserved
                parent.set("children",tentativeChildrenWithPreserved)
            
            allOptions = {}
            for child in parent.get("children"):
                if child.get("options") is not None:
                    for option,value in child.get("options").items():
                        if option not in allOptions:
                            allOptions[option] = [value]
                        else:
                            allOptions[option].append(value)
                        
            parent.set("options",allOptions)    
            parent.set("recordType",recordType)
            parent.set('activeChildCount',functools.reduce(lambda a,b:a+0 if b["isInactive"] else a+1,parent.get("children",[]),0))
            parent.set("childCount",len(parent.get("children",[])))
            
            if (existingRecord):
                for key in parent.data.keys():
                    if key.startswith("_") and parent.get(key) is None:
                        parent.set(key,existingRecord.get(key))
                for shopifyField in ["shopifyId","shopifyGraphQLId"]:
                    if existingRecord.get(shopifyField) is not None:
                        parent.set(shopifyField,existingRecord.get(shopifyField))
                        
            for child in parent.get("children"):
                if child.get("price"):
                    parent.set("_defaultPrice",child.get("price"))
                    
                 
            if parent.get("activeChildCount")<1:
                print("no active children",parent.recordId)
                saveParent = False
                
                print(f"{parent.get('urlComponent')}  has no children!")
            if parent.get("price") is None:
                parent.set("_SpecialOrderItem",True)
                print(f"{parent.get('id')} {parent.get('urlComponent')}  has no pricing!")
                
            if parent.get("urlComponent") is None and False:
                parent.dump()
                sys.exit()
            if saveParent:
            
                finalizedParent = self.finalMapProduct(parent)
                
                
                self.handleMetafields(finalizedParent)
                self.writeConsolidatedRecord(recordId,finalizedParent)
                self.postProcess(self.loadConsolidateRecord(recordId))
            else:
                print("not saving",parent.recordId)
            
    def run(self):        
        for recordType in ["serviceSaleItem","inventoryItem","assemblyItem"]:
            if hasattr(self,"type") and len(self.type)>0 and recordType not in self.type:
                continue
            print(f"Parentizing {recordType}")
            self.processType(recordType)
    def postProcess(self,record):
        pass