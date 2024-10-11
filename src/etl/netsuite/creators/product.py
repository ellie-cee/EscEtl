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
        metafields = record.get("metafields",{})
        for field in record.get("metafields").keys():
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
    def run(self):
        variantGQL = Variants()
        productGQL = Products()
        for record in self.consolidatedRecordList():
            if record.get("shopifyId"):
                continue
            customFields = record.get("customFields")
            metafields = record.get("metafields")
    
            input  = {
                "input":{
                    "title":customFields.get("cwgp_mktg_websitetitle"),
                    "descriptionHtml":customFields.get("cwgp_mktg_mktgdscrptnshrttxt"),
                    "handle":record.get("handle"),
                    "productOptions":[{"name":x,"values":list(map(lambda y:{"name":y},record.get("options")[x]))} for x in record.get("options").keys()],
                    "productType":customFields.get("cwgp_lstproddivision",{}).get("refName"),
                    "seo":{
                        "description":customFields.get("cwgp_mktg_mktgdscrptnshrttxt"),
                        "title":metafields.get("title"),
                    },
                    "status":"DRAFT" if record.get("isInactive") else "ACTIVE",
                    "tags":["t1-432"],
                    "metafields":self.formatMetafields(record)        
                },
            }
    
    
        ret = productGQL.createProduct(input)
    
        productId = ret.search("data.productCreate.product.id")
        variantId = ret.search("data.productCreate.product.variants.nodes[0].id")
        if productId is not None:
            record.set("shopifyId",productId.split("/")[-1])
            record.set("shopifyGraphQLId",productId)
            imret = productGQL.uploadImages(
                {
                    "media":[{"alt":customFields.get("cwgp_mktg_websitetitle"),"mediaContentType":"IMAGE","originalSource":x.replace("media/","https://elecas7.dreamhosters.com/s4/")} for x in record.get("images")],
                    "productId":productId
                }
        )
        firstVariant = True
        for variant in record.get("children"):
            if not variant.get("price"):
                continue
            metafields = {
                "productInformationTab":variant.get("productInformationTab"),
            }
            color = jpath("options.Color",variant)
            if color:
                swatches = self.mappings.get("swatches")
                for swatch in swatches.values():
                    if swatch.source in color.lower():
                        metafields["swatch"] = swatch.id
                
            input = {
                "input":{
                    "productId":productId,
                    "barcode":variant.get("barcode"),
                    "price":variant["price"]["0"].get("retail",variant["price"]["0"]["wholesale"]),
                    "options":list(variant.get("options",{}).values()),
                    "metafields":self.formatMetafields({"metafields":metafields}),
                    "inventoryPolicy":"CONTINUE",
                    "inventoryItem":{
                        "sku":variant.get("SKU"),
                        "requiresShipping":False if record.get("recordType")=="servicSaleItem" else True
                    }
                }                
            }
            if (variant.get("image")):
                fileName = variant.get("image").split("/")[-1].split(".")
                extension = fileName.pop()
                fileName = ".".join(fileName)
                
                #filename = slugify(fileName)
                
                upl = self.uploader.upload_image(
                    f"{self.config('sourceImagePath')}{urllib.parse.quote(fileName.replace('media_',''))}.{extension}"
                )
                if upl is not None:
                    input["input"]["mediaId"] = upl.get("id")
            
            if firstVariant:
                variant["shopifyId"] = variantId.split("/")[-1]
                variant["shopifyGraphQLId"] = variantId
                input = input["input"]
                vci = {
                    "productId":productId,
                    "variants":[
                        {
                            "id":variantId,
                            "optionValues":[{"optionName":k,"name":variant["options"][k]} for k in variant.get("options").keys()],
                            "price":input["price"],
                            "barcode":input["barcode"],
                            "metafields":input["metafields"],
                            "inventoryItem":input["inventoryItem"],
                            "inventoryPolicy":"CONTINUE",
                        }
                    ]
                }
                if input.get("mediaId"):
                    vci["variants"][0]["mediaId"]=input.get("mediaId")
                    
                ret = variantGQL.updateteVariant(vci)
                
                firstVariant = False
            else:
                ret = variantGQL.createVariant(input)
                print(json.dumps(ret,indent=1))
                variantId = ret.search("data.productVariantCreate.productVariant.id")
                if variantId:
                    variant["shopifyId"] = variantId.split("/")[-1]
                    variant["shopifyGraphQLId"] = variantId
        productGQL.publishProduct(productId)
        record.write()
        

            
                