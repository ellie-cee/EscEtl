import asyncio
import json
import os
import pathlib
import sys
import traceback
from .netsuiteClient import NetSuiteClient

class NetsuiteImporter(NetSuiteClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ns = self.netsuiteInit()
        
    def consolidatedRecorFor(self,type):
        return {
            "salesOrder":"order",
            "inventoryItem":"product",
            "assemblyItem":"product",
            "serviceSalesItem":"product",
            "customer":"company"
        }.get(type,"invalidItem")
    def getRecord(self,type,recordId):
        async def getNetsuiteRecord(type,recordId):
            raw = await self.ns.rest_api.get(f"/record/v1/{type}/{recordId}",params={"expandSubResources":True})
            return self.prune(raw)
        def async_to_sync(awaitable):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(awaitable)
        return async_to_sync(getNetsuiteRecord(type,recordId))
    
    async def collectRecords(self,type,expandSubItems=True):
        print(f"Collecting {type}")
        if not pathlib.Path(f"records/{type}").exists():
            pathlib.Path(f"records/{type}").mkdir()
        proceed = True
        offset = 0
        while proceed:
            try:
                records = await self.ns.rest_api.get(f"/record/v1/{type}",params={"offset":offset})
            except KeyboardInterrupt:
                sys.exit(0)
            except:
                traceback.print_exc()
                continue
            for recordToImport in records.get("items"):
                recordId = recordToImport.get("id")
                
                filename = f"records/{type}/{recordId}.json"
                record = self.loadRecord(recordId,type)
                existingConsolidatedRecord = self.loadConsolidateRecord(recordId,self.consolidatedRecorFor(type))

                if record is not None:
                    if not record.get("requiresUpdate"):
                        continue
                if True:
                    running = True    
                    while running:
                        if os.path.exists(filename):
                            if record is not None:
                                if existingConsolidatedRecord is not None:
                                    if existingConsolidatedRecord.get("_orderFulfillmentStatus")=="Fulfilled":
                                        continue
                                elif not record.get("_requiresUpdate"):
                                    continue
                        try:
                            print(f"fetching details: {type}:{recordId}")
                            raw = await self.ns.rest_api.get(f"/record/v1/{type}/{recordId}",params={"expandSubResources":expandSubItems})
                            details = self.prune(raw,type)
                            self.writeRecord(recordId,details,type)
                            running = False
                        except KeyboardInterrupt:
                            sys.exit()
                        except:
                            traceback.print_exc()
                            print("retrying...",file=sys.stderr)
                            continue
            if records.get("hasMore"):
                offset = offset+1000
                #print(f"Fetching{type} > {offset}")
            else:
                proceed = False
    
    def downloadRecords(self,typeList,expandSubItems=True):
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(
            asyncio.gather(
                *[self.collectRecords(x,expandSubItems=expandSubItems) for x in typeList],
            )
        )