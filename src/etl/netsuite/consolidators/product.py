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
                    record["children"] = []
                    parentItems[recordId] = record
        
        # second pass, fill in children
        for recordId in filter(lambda x: x not in parentItems,self.recordList()):
            parent:NetSuiteRecord
            record = self.loadRecord(recordId)
            if "parent" in record and record.search("parent.id") in parentItems:
                if record.get("isInactive",True):
                    continue
                
                parent = parentItems[record.search("parent.id")]
                for ignoreField in ["assetAccount","atpMethod", "autoLeadTime", "autoPreferredStockLevel", "averageCost", "baseUnit", "binNumber", "class", "cogsAccount", "consumptionUnit", "copyDescription", "costEstimateType", "costEstimateUnits", "costingMethod", "currency", "enforceminqtyinternally",  "futurehorizon",  "isDropShipItem", "isGCoCompliant", "isLotItem", "isOnline", "isSerialItem", "isSpecialOrderItem",  "itemType", "itemVendor", "manufacturer", "matrixType", "offerSupport", "parent", "roundUpAsComponent", "seasonalDemand", "shipIndividually", "stockUnit",  "weightUnits"]:
                    if ignoreField in record.data:
                        del record.data[ignoreField]
                        if record.has(ignoreField):
                            delattr(record,ignoreField)
                record.set("productInformationTab", self.mapProductInformation(record))
                customFields = record.get("customFields")
                pricing = None
                if record.get("price") is not None:
                    self.handlePrices(record)
                self.handleOptions(record,parent)  
                
                parent.append("children",record)
        # pass 3 fill in details       
        for recordId in parentItems:
            if pathlib.Path("records/product-{recordId}.json").exists():
                continue
            parent = parentItems[recordId]
            allOptions = {}
            parent.set("childCount",len(list(parent["children"])))
            self.handleImages(parent)
            parent.set("children",[self.finalMapRecord(child) for child in parent.get("children")])
                
            parent.set("recordType",recordType)
            parent.set('activeChildCount',functools.reduce(lambda a,b:a+0 if b["isInactive"] else 1,parent["children"],0))
            saveParent = True        
            if parent["activeChildCount"]<1:
                saveParent = False
                print(f"{parent.get('urlComponent')}  has no children!")
            if not "pricing" in parent:
                parent["SpecialOrderItem"] = True
                print(f"{parent.get('id')} {parent.get('urlComponent')}  has no pricing!")
            if not "urlComponent" in parent:
                saveParent = False
                print(f"Skipping {parent['id']}: {parent.get('customFields').get('cwgp_mktg_websitetitle')}: no URL")
            if saveParent:
                finalizedParent = self.finalMapRecord(parent)
                self.handleMetafields(finalizedParent)
                self.writeConsolidatedRecord(recordId,finalizedParent)
                
            
    def run(self):        
        for recordType in ["serviceSaleItem","inventoryItem","assemblyItem"]:
            print(f"Parentizing {recordType}")
            self.processType(recordType)