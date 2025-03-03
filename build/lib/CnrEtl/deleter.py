import asyncio
import json
import os
import pathlib
import sys
import traceback
from .netsuite import *
import argparse

class RecordDeleter:
    def __init__(self):
        
        parser = argparse.ArgumentParser()
        parser.add_argument("--only","-o",action="append",default=[])
        parser.add_argument("--function","-f",required=True)
        parser.add_argument("--batch","-b",action="store_true")
        parser.add_argument("--selector","-s",default=None)
        print("dewdew")
        
        for key,val in parser.parse_args()._get_kwargs():
            setattr(self,key,val)
        if self.selector is not None:
            setattr(self,"only",getattr(self,self.selector)())
        self.consolidator = NetSuiteClient()    
    def run(self):
        if self.batch:
            getattr(self,self.function)()
        else:
            record:NetSuiteRecord
            for record in self.records():
                if len(self.only)>0 and record.recordId not in self.only:
                    continue
                
    def records(self):
        return self.consolidator.consolidatedRecordList()
    
