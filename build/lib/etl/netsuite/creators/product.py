import json
from ..netsuiteClient import *
from etl.graphQL import Order,Variants,Products,MetaField
from etl.misc import *
from shopify_uploader import ShopifyUploader
import urllib.parse

class ProductCreator(ProductRecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
        self.uploader = ShopifyUploader(self.config('shopifyToken'),self.config("shopifySite"))
        
    
        
    def formatMetafields(self,record):
        ret = []
        mappings = self.mappings["metafields"]
        metafields = record.get("_metafields",{})
        for field in record.get("_metafields",{}).keys():
            if field not in ["compatibleItems","requiredItems"]:
                if field in mappings:
                    mapping = mappings[field]
                    ret.append({
                        "key":mapping.key,
                        "namespace":mapping.namespace,
                        "type":mapping.type,
                        "value":json.dumps(metafields.get(field)) if type(metafields.get(field)) is not str else metafields.get(field)
                    })
        return ret
    def getMetafield(self,record,key,default=None):
        if record.get("_metafields") is None:
            return None
        for field,value in record.get("_metafields",{}).items():
            if field==key:
                return value
        return None
        
    def run(self):
        variantGQL = Variants()
        productGQL = Products()
        onlineStoreChannel = productGQL.getChannelByName("Online Store")
        for record in self.consolidatedRecordList():
            if hasattr(self,"only") and len(self.only)>0 and record.recordId not in self.only:
                continue       
            if record.get("shopifyId"):
                continue
            shopifyId = productGQL.getProductByHandle(record.get("handle"))
            if shopifyId is not None:
                record.set("shopifyGraphQLId",shopifyId)
                record.set("shopifyId",shopifyId.split("/")[-1])
                record.write()
                print(f"Skipping {record.recordId}")
                continue
            print(record.recordId)
            customFields = record.get("customFields")
            metafields = record.get("metafields")
            productOptions = {}
            
            input  = {
                "input":{
                    "title":customFields.get("cwgp_mktg_websitetitle"),
                    "descriptionHtml":customFields.get("cwgp_mktg_mktgdscrptnshrttxt"),
                    "handle":record.get("handle"),
                    
                    "productOptions":[{"name":x,"values":list(map(lambda y:{"name":y},list(set(record.get("options")[x]))))} for x in record.get("options").keys()],
                    "productType":customFields.get("cwgp_lstproddivision",{}).get("refName"),
                    "seo":{
                        "description":customFields.get("cwgp_mktg_mktgdscrptnshrttxt"),
                        "title":self.getMetafield(record,"title"),                    },
                    "status":"DRAFT" if record.get("isInactive") else "ACTIVE",
                    "tags":record.get("_tags",[]),
                    "metafields":self.formatMetafields(record)
                            
                },
            }
    
            
    
            ret = productGQL.createProduct(input)
            
            
        
            productId = ret.search("data.productCreate.product.id")
            variantId = ret.search("data.productCreate.product.variants.nodes[0].id")
            if False:
                variantGQL.deleteVariants({
                    "productId":productId,
                    "variantsIds":[variantId]
                })
            if productId is not None:
                record.set("shopifyId",productId.split("/")[-1])
                record.set("shopifyGraphQLId",productId)
                
                
                imret = productGQL.uploadImages(
                    {
                        "media":[{"alt":customFields.get("cwgp_mktg_websitetitle"),"mediaContentType":"IMAGE","originalSource":f"{self.config('sourceImagePath')}{x.replace('media/','')}"} for x in record.get("images")],
                        "productId":productId
                    }
                )
                firstVariantProcessed = False
                for variant in record.get("children"):
                    print(f"Adding Variant {variant.get('SKU')}")
                    if not variant.get("price"):
                        continue
                    metafields = variant.get("_metafields",{})
                    metafields["productInformationTab"]=variant.get("productInformationTab"),
                    variant["_metafields"] = metafields
                    
                    color = jpath("options.Color",variant)
                    if color:
                        swatches = self.mappings.get("swatches")
                        for swatch in swatches.values():
                            if swatch.source in color.lower():
                                metafields["swatch"] = swatch.id
                    optionString = " ".join(list(variant.get("options",{}).values()))
                    if optionString=="":
                        optionString = "Default Title"    
                    options = []
                   
                    input = {
                        "productId":productId,
                        "variants":[{                            
                            "barcode":variant.get("barcode"),
                            "price":variant["price"]["0"].get("retail",variant["price"]["0"]["wholesale"]),
                            "optionValues":[{"name":value,"optionName":key} for key,value in variant.get("options",{}).items()],
                            "metafields":self.formatMetafields(variant),
                            "inventoryPolicy":"DENY",
                            "inventoryItem":{
                                "sku":variant.get("SKU"),
                                "requiresShipping":False if record.get("recordType")=="servicSaleItem" else True,
                                "tracked":False if record.get("recordType")=="servicSaleItem" else True,
                            }
                        }]      
                    }
                    
                    if (variant.get("image")):  
                        upl = self.uploader.upload_image(
                            f"{self.config('sourceImagePath')}{urllib.parse.quote(variant.get('image').replace("media/",""))}",
                            alt=optionString,
                            check=False
                        )
                        if upl is not None:
                            input["variants"][0]["mediaId"] = upl.get("id")
                    
                    
                    
                    if not firstVariantProcessed:
                        input["variants"][0]["id"] = variantId
                        ret = variantGQL.updateteVariant(input)
                        variant["shopifyId"] = variantId.split("/")[-1]
                        variant["shopifyGraphQLId"] = variantId
                        variant["_inventoryItemId"] = ret.search("data.productVariantsBulkUpdate.productVariants[0].inventoryItem.id")
                        firstVariantProcessed = True
                    else:    
                        ret = variantGQL.createVariants(input)
                        ret.dump()
                        variantId = ret.search("data.productVariantsBulkCreate.productVariants[0].id")
                        if variantId:
                            variant["shopifyId"] = variantId.split("/")[-1]
                            variant["shopifyGraphQLId"] = variantId
                            variant["_inventoryItemId"] = ret.search("data.productVariantsBulkCreate.productVariants[0].inventoryItem.id")
                productGQL.publishProduct(productId,onlineStoreChannel.get("id"))
                record.write()
                self.postProcess(record)
            else:
                ret.dump()
    def defaultInventoryPolicy(self):
        return "DENY"
            
                