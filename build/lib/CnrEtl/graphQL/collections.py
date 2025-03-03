from .base import *

class Collections(GraphQL):
    
    def create(self,input):
        return self.run(
            """
            mutation createCollectionMetafields($input: CollectionInput!) {
                collectionCreate(input: $input) {
                    collection {
                        id
                        metafields(first: 3) {
                            nodes {
                                id
                                namespace
                                key
                                value
                            }
                        }
                    }
                    userErrors {
                        message
                        field
                    }
                }
            }
            """,
            input
        )