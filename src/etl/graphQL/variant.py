from .base import *

class Variants(GraphQL):
    def get(self,variantId):
        return self.run(
            """
            query getVariant($id:ID!) {
                productVariant(id:$id) {
                    id
                }
            }
            """,
            {"id":variantId}
        )
    def createVariant(self,input):
        return self.run(
            """
            mutation createProductVariantMetafields($input: ProductVariantInput!) {
                productVariantCreate(input: $input) {
                    productVariant {
                        id
                        metafields(first: 3) {    
                            nodes {
                                id
                                namespace
                                key
                                value
                            }
                        }
                        image {
                            url
                            id
                        }
                    }
                    userErrors {
                        message
                        field
                    }
                }
            }
            """,
            input)
    def deleteVariants(self,input):
        return self.run(
            """
            mutation productVariantsBulkDelete($productId: ID!, $variantsIds: [ID!]!) {
                productVariantsBulkDelete(productId: $productId, variantsIds: $variantsIds) {
                    product {
                        id
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            input
        )
    def updateteVariant(self,input):
    
        return self.run(
                """
                mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
                    productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                        product {
                            id
                        }
                        productVariants {
                            id
                            metafields(first: 2) {
                                nodes {
                                    namespace
                                    key
                                    value
                                }
                            }
                            image {
                                url
                            }
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }
                """,
                input)
        