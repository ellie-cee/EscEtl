from .base import *

class Products(GraphQL):
    def delete(self,productId):
        
        return self.run(
            """
            mutation deleteProduct($id:ID!) {
                productDelete(input:{id:$id}) {
                    deletedProductId
                        userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {
                "id":productId,
                "synchrous":True
            }
        )
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
                        createdAt
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
                        options(first:3) {
                            id
                            name
                            values
                            position
                        }
                        variants(first:100) {
                            nodes {
                                id
                                title
                                displayName
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
                                metafields(first:10) {
                                    nodes {
                                        type
                                        namespace
                                        key
                                        value
                                    }
                                }
                                inventoryItem {
                                    id
                                }
                                inventoryPolicy
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
    def get(self,productId):
        return self.run(
            """
            query getProductMedia($id:ID!) {
                product(id:$id) {
                    id
                    handle
                    tags
                    options(first:3) {
                            id
                            name
                            values
                            position
                        
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
                        options(first:3) {
                            name
                            values
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
                    #{"publicationId":"gid://shopify/Publication/138350559472"},
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
    def getProductByHandle(self,handle):
        ret = self.run(
            """
            query getByHandle($query:String!) {
                products(first:5,query:$query) {
                    nodes {
                        id
                        handle
                    }
                }
            }
            """,
            {"query":f"handle:{handle}"}
        )
        product = next(filter(lambda x:x.get("handle")==handle.lower(),ret.search("data.products.nodes",[])),None)
        if product is not None:
            return product.get("id")
        return None
    def getChannelByName(self,name):
        return next(filter(lambda x: name in x.get("name"),self.getChannels().search("data.channels.nodes",[])),None)
        
    def getChannels(self):
        return self.run(
            """
            query getChannels {
                channels(first:25) {
                    nodes {
                        id
                        name
                        handle
                    }
                }
            }
            """
        )
    def getPriceLists(self):
        return self.run(
            """
            query {
                priceLists(first:20) {
                    nodes {
                        catalog {
                            title
                        }
                        id
                        name
                        parent {
                            adjustment {
                                type
                                value
                            }
                        }    
                    }
                    
                }
            }
            """
        )
    def updateProduct(self,input):
        return self.run(
            """
            mutation UpdateProductWithNewMedia($input: ProductInput!, $media: [CreateMediaInput!]) {
                productUpdate(input: $input, media: $media) {
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
    def updatePriceList(self,input):
        return self.run(
            """
            mutation priceListUpdate($id: ID!, $input: PriceListUpdateInput!) {
                priceListUpdate(id: $id, input: $input) {
                    priceList {
                        id
                        parent {
                            adjustment {
                                type
                                value
                            }
                        }
                    }
                    userErrors {
                        message
                        field
                        code
                    }
                }
            }
            """,
            input
        )
    def updateOption(self,input):
        return self.run(
            """
            mutation updateOption($productId: ID!, $option: OptionUpdateInput!) {
                productOptionUpdate(productId: $productId, option: $option) {
                    userErrors {
                       field
                        message
                        code
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
                    
                }
            } 
            """,
            input
        )
        
        