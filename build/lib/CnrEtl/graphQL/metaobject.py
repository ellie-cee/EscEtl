from .base import *

class MetaObject(GraphQL):
    def publish(self,id):
        return self.run(
            """
            mutation UpdateMetaobject($id: ID!, $metaobject: MetaobjectUpdateInput!) {
                metaobjectUpdate(id: $id, metaobject: $metaobject) {
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
            {
                "id":id,
                "metaobject":{
                    "capabilities":{
                        "publishable":{
                            "status":"ACTIVE"
                        }
                    }
                }
            }
        )
    def upsert(self,handle,type,input):
        return self.run(
            """
            mutation CreateMetaobject($handle: MetaobjectHandleInput!,$metaobject:MetaobjectUpsertInput!) {
                metaobjectUpsert(handle: $handle,metaobject:$metaobject) {
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
            {
                "handle":{
                    "type":type,
                    "handle":handle,
                },
                "metaobject":input
                
            }
        )
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
    def getAllByType(self,objectType):
        return self.iterable(
            """
            query getMetaobjectsByType($type:String!,$after"String) {
                metaobjects(type:$type,first:100,after:$after) {
                    nodes {
                        handle
                        id
                        capabilities {
                            publishable {
                                status
                            }
                        }   
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {"type":objectType}
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
    def delete(self,id):
        return self.run(
            """
            mutation DeleteMetaobject($id: ID!) {
                metaobjectDelete(id: $id) {
                    deletedId
                    userErrors {
                        field
                        message
                        code
                    }
                }
            }
            """,
            {"id":id}
            
        )
    
    