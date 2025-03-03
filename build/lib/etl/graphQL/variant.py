from .base import *

class Variants(GraphQL):
    def getAll(self):
        return self.iterable(
            """
            query getVariants($after:String) {
                variants(first:250) {
                    nodes {
                        id
                        inventoryItem {
                            id
                        }
                        product {
                            id
                            netSuiteId: metafield(namespace:"cnr",key:"netsuite_id) {
                                value
                            }
                        }
                        sku
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {}
        )
    def inventoryActivate(self,inventoryItemId,locationId,quantity):
        return self.run(
            """
            mutation ActivateInventoryItem($inventoryItemId: ID!, $locationId: ID!, $available: Int) {
                inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId, available: $available) {
                    inventoryLevel {
                        id
                        quantities(names: ["available"]) {
                            name
                            quantity
                        }
                        item {
                            id
                        }
                        location {
                            id
                        }
                    }
                }
            }
            """,
            {
                "available":quantity,
                "inventoryItemId":inventoryItemId,
                "locationId":locationId
            }
        )
    def updateInventory(self,input):
            return self.run(
            """
               mutation InventorySet($input: InventorySetQuantitiesInput!) {
                    inventorySetQuantities(input: $input) {
                        inventoryAdjustmentGroup {
                            createdAt
                            reason
                            referenceDocumentUri
                            changes {
                                name
                                delta
                            }
                        }
                        userErrors {
                            field
                            message
                        }
                    }
                }
                """,
                {"input":input}
            )
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
    def createVariants(self,input):
        return self.run(
            """
            mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
                productVariantsBulkCreate(productId: $productId, variants: $variants) {
                    userErrors {
                        field
                        message
                    }
                    product {
                        id
                        options {
                            id
                            name
                            values
                            position
                            optionValues {
                            id
                            name
                            hasVariants
                            }
                        }
                    }
                    productVariants {
                        id
                        title
                        selectedOptions {
                            name
                            value
                        }
                        inventoryItem {
                            id
                        }
                    }
                }
            }
            """,
            input
        )    
    def createVariant(self,input):
        return self.run(
            """
            mutation createProductVariantMetafields($input: ProductVariantInput!) {
                productVariantCreate(input: $input) {
                    productVariant {
                        id
                         selectedOptions {
                            name
                            value
                        }
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
                        inventoryPolicy
                        inventoryItem {
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
                            inventoryPolicy
                            inventoryItem {
                                id
                            }
                            selectedOptions {
                                name
                                value
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
        