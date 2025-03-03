from ..base import *

class MagentoRecord(BaseRecord):
    pass
class MagentoConsolidatedRecord(MagentoRecord):
    pass

class MagentoClient(BaseClient):
    def __init__(self, **kwargs):
           super().__init__(**kwargs)
        
    def loadRecord(self,id,type="inventoryItem",prune=True,searchable=False) -> MagentoRecord:
        return MagentoRecord.load(id,type)
    def recordExists(self,id,type):
        return MagentoRecord.exists(id,type)
    def consolidatedRecordExists(self,id,type):
        return MagentoConsolidatedRecord.exists(id,type)
        
    def writeRecord(self,id,data,type):
        MagentoRecord(id,data,type).write()
    def loadConsolidateRecord(self,id,type="product",searchable=False) -> MagentoConsolidatedRecord:
        return MagentoConsolidatedRecord.load(id,type)
    def writeConsolidatedRecord(self,id,data,type):
        MagentoConsolidatedRecord(id,data,type).write()
    def recordList(self,type):
        return MagentoRecord.list(type)
    def consolidatedRecordList(self,type) -> List[MagentoConsolidatedRecord]:
        return list(
            map(
                lambda x:MagentoConsolidatedRecord(x),
                MagentoConsolidatedRecord.list(type)
            )
        )
    def consolidatedRecordIds(self,type):
        return MagentoConsolidatedRecord.list(type)
    
class CustomerRecordAwareClient(MagentoClient):
    pass
class ProductRecordAwareClient(MagentoClient):
    pass
class OrderRecordAwareClient(MagentoClient):
    pass