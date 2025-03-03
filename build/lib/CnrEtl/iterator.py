import asyncio
import json
import math
import os
import pathlib
import sys
import time
import traceback
from .netsuite import *
import argparse

class RecordIterator:
    def __init__(self):
        
        parser = argparse.ArgumentParser()
        parser.add_argument("--only","-o",action="append",default=[])
        parser.add_argument("--function","-f",required=True)
        parser.add_argument("--batch","-b",action="store_true")
        parser.add_argument("--selector","-s",default=None)
        parser.add_argument("--param","-p",action="append",default=[])
        
        args = parser.parse_args()._get_kwargs()
        
        self.paramData = {}
        
        for key,val in parser.parse_args()._get_kwargs(): 
            setattr(self,key,val)
            
        if hasattr(self,"param"):
            self.paramData = {}
            for param in self.param:
                parts = list(map(lambda x:x.strip(),param.split("=")))
                
                key =  parts[0]
                val = parts[1]
                self.paramData[key] = val.split(",") if "," in val else val
                
        if self.selector is not None:
            setattr(self,"only",getattr(self,self.selector)())
            
        self.consolidator = NetSuiteClient()
        
                
    def params(self,key,default=None):
        data = self.paramData.get(key)
        return default if not data else data
    
    def run(self):
        if self.batch:
            getattr(self,self.function)()
        else:
            record:NetSuiteRecord
            records = self.records()
            remaining = len(records)
            count = 1
            persec = 0
            if len(self.only)>0:
                
                remaining = len(self.only)
            start = int(math.floor(time.time()))
            for record in self.records():
                if len(self.only)>0 and record.recordId not in self.only:
                    continue
                
                getattr(self,self.function)(record)
                end = int(math.floor(time.time()))
                
                count = count+1
                persec = (math.floor(((end-start)/count)*100))/100
    def records(self):
        return self.consolidator.consolidatedRecordList()
    
class CompanyIterator(RecordIterator):
    def __init__(self):
        super().__init__()
        self.companyGql = Companies()
        self.orderGql = Order()
        
class ProductIterator(RecordIterator):
    def __init__(self):
        super().__init__()
        self.productGql = Products()
        self.variantGql = Variants()
class OrderIterator(RecordIterator):
    def __init__(self):
        super().__init__()
        self.orderGql = Order()
        self.companyGql = Companies()
        