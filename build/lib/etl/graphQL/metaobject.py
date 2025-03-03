from .base import *

class MetaObject(GraphQL):
    def create(self,input):
        return self.run(
            """
            mutation CreateMetaobject($metaobject: MetaobjectCreateInput!) {
                metaobjectCreate(metaobject: $metaobject) {
                    metaobject {
                       handle
                       id
                    }
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }
            """,
            input
        )
    def getByType(self,objectType):
        return self.run(
            """
            query getMetaobjectsByType($type:String!) {
                metaobjects(type:$type,first:100) {
                    nodes {
                        handle
                        id
                        name: field(key:"name") {
                            value
                        }
                    }
                }
            }
            """,
            {"type":objectType}
        )
    def swatches(self):
        return self.run(
            """
            query getSwatchMetaobjects   {
                metaobjects(type:"variant_swatch",first:100) {
                    nodes {
                        
                        handle
                        id
                        name: field(key:"name") {
                            value
                        }
                        color: field(key:"color") {
                            value
                        }
                        image: field(key:"swatch_image") {
                            
                            image: reference {
                                ... on MediaImage {
                                   id
                                   image {
                                       url
                                   }
                                }
                            }
                        }
                    }
                }
            }
            """,
            {}
        )
    
    