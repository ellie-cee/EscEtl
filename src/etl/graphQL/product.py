from .base import *

class Products(GraphQL):
    def listProducts(self):
        return self.iterable(
            """
            query getProducts($first:Int!,$after:String) {
                products(first:$first,after:$after) {
                    nodes {
                        id
                        title
                        descriptionHtml
                        handle
                        productType
                        storeUrl: onlineStoreUrl
                        previewUrl: onlineStorePreviewUrl
                        priceRange: priceRangeV2 {
                            minVariantPrice {
                                amount
                            }
                            maxVariantPrice {
                                amount
                            }
                        }
                        seo {
                            description
                            title
                        }
                        tags
                        vendor
                        metafields(first:50) {
                            nodes {
                                type
                                namespace
                                key
                                value
                            }
                        }
                        media(first:25) {
                            nodes {
                                preview {
                                   image {
                                        previewUrl: url
                                    }
                                }
                            }
                        }
                        variants(first:100) {
                            nodes {
                                id
                                title
                                sku
                                barcode
                                price
                                selectedOptions {
                                    optionName: name
                                    optionValue: value
                                }
                                image {
                                    url
                                }
                                metafields(first:20) {
                                    nodes {
                                        type
                                        namespace
                                        key
                                        value
                                    }
                                }
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
            {"first":75},
            dataroot="data.products"
        )
    def productMedia(self,productId):
        return self.run(
            """
            query getProductMedia($id:ID!) {
                product(id:$id) {
                    media(first:100) {
                        nodes {
                            id    
                        }
                        
                    }
                }
            }
            """,
            {"id":productId}
        )
    def deleteMedia(self,productId,mediaIds):
        return self.run(
            """
            mutation productDeleteMedia($mediaIds: [ID!]!, $productId: ID!) {
                productDeleteMedia(mediaIds: $mediaIds, productId: $productId) {
                    deletedMediaIds
                    deletedProductImageIds
                    mediaUserErrors {
                        field
                        message
                    }
    
                }
            }
            """,
            {
                "mediaIds":mediaIds,
                "productId":productId
            }
        )
        
        
    def createProduct(self,input):
        return self.run(
            """
            mutation createProductMetafields($input: ProductInput!) {
                productCreate(input: $input) {
                    product {
                        id
                        metafields(first: 3) {
                            nodes {
                                id
                                namespace
                                type
                                key
                                value
                            }
                        }
                        media(first:30) {
                            nodes {
                                id
                                preview {
                                    image {
                                        url    
                                    }
                                }
                            }
                        }
                        variants(first:1) {
                            nodes {
                                id
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
    def assignMedia(self,input):
        return self.run(
                """
            mutation productCreateMedia($media: [CreateMediaInput!]!, $productId: ID!) {
                productCreateMedia(media: $media, productId: $productId) {
                    media {
                        alt
                        mediaContentType
                        status
                    }
                    mediaUserErrors {
                        field
                        message
                    }
                    product {
                        id
                        title
                    }
                }
            }
            """,
            input
        )
    def publishProduct(self,productId,channelId):
        #channelId = "gid://shopify/Publication/138350493936"
        return self.run(
        """
        mutation productPublish($input: ProductPublishInput!) {
            productPublish(input: $input) {
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
        {
            "input":{
                "id":productId,
                "productPublications":[
                    {"publicationId":channelId},
                    {"publicationId":"gid://shopify/Publication/138350559472"},
                ]
            }
        })
    def uploadImages(self,input):
        return self.run(
            """
            mutation productCreateMedia($media: [CreateMediaInput!]!, $productId: ID!) {
                productCreateMedia(media: $media, productId: $productId) {
                    media {
                        alt
                        mediaContentType
                        status
                    }
                    mediaUserErrors {
                        field
                        message
                    }
                    product {
                        id
                        title
                    }
                }
            }
            """,
            input
        )
    def getProductVariants(self,productId):
        return self.run(
            """
            query getVariants($id:ID!) {
                product(id:$id) {
                    variants(first:100) {
                        nodes {
                            id
                            sku
                        }
                    }
                }
            }
            """,
            {"id":productId}
        )
    def getProductName(self,productId):
        return self.run(
            """
            query getVariants($id:ID!) {
                product(id:$id) {
                    title
                }
            }
            """,
            {"id":productId}
        ).search("data.product.title")
    def deleteVariants(self,productId,variants):
        return self.run(
            """
            mutation productVariantsBulkDelete($productId: ID!, $variantsIds: [ID!]!) {
                productVariantsBulkDelete(productId: $productId, variantsIds: $variantsIds) {
                    product {
                        id
                        variants(first:100) {
                            nodes {
                                id
                            }
                        }
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {
                "productId":productId,
                "variantsIds":variants
            }
        )
        
        