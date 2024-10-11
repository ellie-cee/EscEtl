from .base import *
from ..misc import partition

class MetaField(GraphQL):
    maxFields = 25
    def upset(self,payload):
        if len(payload)>self.maxFields:
            for chunk in partition(payload,chunksize=25):
                return self.pushFields(chunk)
        else:
            return self.pushFields(payload)
        
    def pushFields(self,payload):
        return self.run(
            """
            mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
                metafieldsSet(metafields: $metafields) {
                    metafields {
                        key
                        namespace
                        value
                        createdAt
                        updatedAt
                    }
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }""",
            payload
        )
        

    