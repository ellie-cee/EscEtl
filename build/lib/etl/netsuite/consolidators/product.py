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

    def finalMapRecord(self,record):
        ret = {}
        data = record
        mapping = self.mapping("fieldNames")
        if type(record) is NetSuiteRecord:
            data = record.data
        for field in data.keys():
            if field in mapping:
                ex = mapping.get(field)
                if ex.ignore=="y":
                    continue
                fn = ex.oto if ex.oto is not None else field
                if ex.path:
                    ret[fn] = jpath(ex.path,data.get(field))
                else:
                    ret[fn] = data.get(field)
            elif field.startswith("_"):
                ret[field] = data.get(field)
        return ret

            
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
        for recordId in self.recordList():
            
            existingRecord = self.loadConsolidateRecord(recordId)
            if existingRecord and existingRecord.get("shopifyId"):
                if self.has("reprocessProducts"):
                    if self.reprocessRecords is list and len(self.reprocessRecords)>0:
                        if not recordId in self.reprocessRecords:
                            continue
            
            record = self.loadRecord(recordId)
            
            existingRecord = self.loadConsolidateRecord(recordId)
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
                    continue
                
                parent = parentItems[record.search("parent.id")]
                for ignoreField in ["assetAccount","atpMethod", "autoLeadTime", "autoPreferredStockLevel", "averageCost", "baseUnit", "binNumber", "class", "cogsAccount", "consumptionUnit", "copyDescription", "costEstimateType", "costEstimateUnits", "costingMethod", "currency", "enforceminqtyinternally",  "futurehorizon",  "isDropShipItem", "isGCoCompliant", "isLotItem", "isOnline", "isSerialItem", "isSpecialOrderItem",  "itemType", "itemVendor", "manufacturer", "matrixType", "offerSupport", "parent", "roundUpAsComponent", "seasonalDemand", "shipIndividually", "stockUnit",  "weightUnits"]:
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
            if hasattr(self,"only") and len(self.only)>0 and recordId not in self.only:
                continue       
            existingRecord = self.loadConsolidateRecord(recordId)
            if existingRecord is not None and existingRecord.get("shopifyId") is not None:
                continue
            
            parent = parentItems[recordId]
            allOptions = {}
            
            self.handleImages(parent)
            
            tentativeChildren = [self.finalMapRecord(child) for child in parent.get("children")]
            
                        
            hasOptions = False
            for child in tentativeChildren:
                if len(child.get("options").keys())>0:
                    hasOptions = True
                    break
            if hasOptions:
                
                finalChildren = []
                variantsByOptions = {}
                for child in tentativeChildren:
                    
                    childId = int(child.get("netSuiteId"))
                    options = child.get("options")
                    if hasOptions and len(list(options.keys()))<1:
                        continue
                    signature = "|".join(list(map(lambda k:f"{k}-{options[k]}",sorted(list(options.keys())))))
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
                parent.set("children",tentativeChildren)
                
            allOptions = {}
            for child in parent.get("children"):
                for option,value in child.get("options").items():
                    if option not in allOptions:
                        allOptions[option] = [value]
                    else:
                        allOptions[option].append(value)
            parent.set("options",allOptions)    
            parent.set("recordType",recordType)
            parent.set('activeChildCount',functools.reduce(lambda a,b:a+0 if b["isInactive"] else a+1,parent.get("children",[]),0))
            parent.set("childCount",len(parent.get("children",[])))
            for child in parent.get("children"):
                if child.get("price"):
                    parent.set("_defaultPrice",child.get("price"))
                    
            saveParent = True        
            if parent.get("activeChildCount")<1:
                saveParent = False
                print(f"{parent.get('urlComponent')}  has no children!")
            if parent.get("price") is None:
                parent.set("_SpecialOrderItem",True)
                print(f"{parent.get('id')} {parent.get('urlComponent')}  has no pricing!")
                
            if parent.get("urlComponent") is None:
                if self.checkForProductById(parent.get("id")):
                    parent.set("_originalUrlComponent",parent.get("id"))
                    parent.set("urlComponent",slugify(parent.search("customFields.cwgp_mktg_websitetitle")))
                else:
                    saveParent = False
                    print(f"Skipping {parent.get('id')}: {parent.get('customFields').get('cwgp_mktg_websitetitle')}: no URL")
            if saveParent:
                finalizedParent = self.finalMapRecord(parent)
                self.handleMetafields(finalizedParent)
                self.writeConsolidatedRecord(recordId,finalizedParent)
                self.postProcess(self.loadConsolidateRecord(recordId))
                
            
    def run(self):        
        for recordType in ["serviceSaleItem","inventoryItem","assemblyItem"]:
            if hasattr(self,"type") and len(self.type)>0 and recordType not in self.type:
                continue
            print(f"Parentizing {recordType}")
            self.processType(recordType)