from .base import *
from ..misc import partition

class MetaField(GraphQL):
    maxFields = 25
    def upset(self,payload):
        
        if len(payload)>self.maxFields:
            for chunk in partition(payload,chunksize=25):
                ret = self.pushFields({"metafields":chunk})
                ret.dump(printIt=True)
        else:
            return self.pushFields({"metafields":payload}).dump()
    def delete(self,metafieldId):
        return self.run(
            """
            mutation metafieldDelete($input: MetafieldDeleteInput!) {
                metafieldDelete(input: $input) {
                    deletedId
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {
                "input":{
                    "id":metafieldId
                }
            }
        )
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
        

    