from etl.graphQL import *
import json
from ..netsuiteClient import *
from slugify import slugify
from etl.misc import *
import functools



class OrderConsolidator(OrderRecordAwareClient,AddressAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.only = []
        
    def orderCustomFields(self):
        return []
    def mapCustomFields(self,record):
        ret = []
        for field,value in record.get("customFields").items():
            if field in self.orderCustomFields():    
                if value is not None:
                    if type(value) is dict:
                        if "refName" in value:
                            ret.append({"key":field,"value":str(value.get("refName"))})
                    else:
                        ret.append({"key":field,"value":str(value)})
                    
        return ret
        
    def run(self):
        self.productRef = json.load(open("records/consolidated/product/tree.json"))
        self.variantsFound = {}
        self.variantsNotFound = {}
        for recordId in self.recordList():
            
            if self.has("onlyTheseIds"):
                if len(self.onlyTheseIds)>0 and recordId not in self.onlyTheseIds:
                    continue
            record = self.loadRecord(recordId)
            if record is None:
                print(" no recordId")
                continue
            try:
                order = self.processOrder(record)
                if order is None:
                    continue
                existingRecord = self.loadConsolidateRecord(record.recordId)
                if order is not None:
                    
                    if order.get("orderFulfillmentStatus")=="Partially Fulfilled":
                        record.set("requiresUpdate",True)
                    else:
                        record.set("requiresUpdate",False)
                    record.write()
                    
                    if existingRecord is not None and hasattr(existingRecord,"data"):
                        if existingRecord.get("shopifyId") is not None:
                            order["shopifyId"] = existingRecord.get("shopifyId")
                            order["shopifyGraphQLId"] = existingRecord.get("shopifyGraphQLId")
                            order["_requiresFulfillment"] = True
                    self.writeConsolidatedRecord(recordId,order)
            except SystemExit:
                sys.exit()
                pass
            except KeyboardInterrupt:
                sys.exit()
            except:
                traceback.print_exc()
                print(json.dumps(record.data))
                sys.exit()
        print(f"found {len(list(self.variantsFound.keys()))}\nnot found {len(list(self.variantsNotFound.keys()))}")       
    def getVariantIdByExternalId(self,externalId):
        if externalId in self.productRef.get("variants"):
            product = self.loadSupportingConsolidateRecord(self.productRef["variants"][externalId],"product")
            if product is None:
                return None
            else:
                variant = None
                for child in product.get("children"):
                    if child.get("netSuiteId")==externalId:
                        return child
        return None
    def getParentNetsuiteProduct(self,externalId):
        if externalId in self.productRef.get("variants"):
            for type in ["inventoryItem","assemblyItem","serviceSalesItem"]:
                if self.recordExists(externalId,type):
                    return self.loadSupportingRecord(self.productRef["variants"][externalId],"inventoryItem")
        return None
            
    def lineItemQuantity(self,lineItem):
        return int(lineItem.get("quantity"))
    def lineItemPrice(self,lineItem):
        return lineItem.get("rate")
    def lineItemTitle(self,lineItem,parent):
        base = parent.get("storeDisplayName",parent.get('description',lineItem.get("itemDescription","")))
        if len(base)>128:
            base = base[0:128]
        
        return base
                
    def processOrder(self,incomingRecord:NetSuiteRecord):
            
            record:NetSuiteRecord = incomingRecord
            order = {
                "sourceIdentifier":record.recordId,
                "sourceName":record.get("tranId",""),
                "lineItems":[],
                "processedAt":record.get("createdDate","1970-01-01T00:00:01"),
                "tags":["_imported-from-netsuite"],
                "customAttributes":self.mapCustomFields(record),
                "note":f"imported from netsuite {record.recordId}",
                "metafields":[
                    {
                        "key":"netsuite_id",
                        "namespace":"cnr",
                        "value":str(record.recordId),
                        "type":"single_line_text_field"
                    }
                ],
                "presentmentCurrency":"USD",
                "_status":record.search("status.refName"),
                "_entityId":record.search("entity.id")
            }
            if record.get("shippingCost",0)>0:
                order["shippingLines"]=[{
                        "priceSet":{
                            "shopMoney":{
                                "currencyCode":"USD",
                                "amount":record.get("shippingCost",0)
                            }
                        },
                        "title":record.search("shipMethod.refName","Shipping"),
                    }]
            discounts = []
            discountLabels = []
            
            billingAddress = self.mapAddress(record.get("billingAddress"),remap=True)
            shippingAddress = self.mapAddress(record.get("shippingAddress"),remap=True)
            if shippingAddress is None:
                shippingAddress = billingAddress
            elif billingAddress is None:
                billingAddress = shippingAddress
                
            if billingAddress:
                order["billingAddress"] = billingAddress
            if shippingAddress:
                order["shippingAddress"] = shippingAddress
            
           
            
            trackingCodes = []
            if record.get("linkedTrackingNumbers"):
                for code in list(reversed(record.get("linkedTrackingNumbers").split(" "))):
                    if code not in trackingCodes:
                        trackingCodes.append(code)
            order["_trackingCodes"] = trackingCodes
                
            customerId = None
            company = self.loadConsolidateRecord(record.search("entity.id"),forceType="company")
            
            if company:
                order["companyLocationId"] = company.search("companyLocation.shopifyId")
                
                contact = next(filter(lambda x:x.get("email")==record.get("email"),company.get("contacts")),None)
                if contact is None:
                    contact = next(filter(lambda x:x.get("email") is not None and x.get("shopifyustomerId") is not None,company.get("contacts")),None)
                    if contact is None:
                        print(f"No contacts for order {record.recordId}")
                        return None
                    else:   
                        order["customerId"] = contact.get("shopifyCustomerId")
            else:
                candidate = self.loadConsolidateRecord(record.search("entity.id"),forceType="customer")
                if candidate is not None:
                    order["customerId"] = candidate.get("shopifyId")
            if customer is None:
                print(f"No customer for order {record.recordId}")
                return 
                return None
            
            
            lineItems = []
            for index,lineItem in enumerate(record.get("item")):
                
                trackingCode = None
                if lineItem.get("isClosed"):
                    continue
                
                try:
                    if not lineItem.get("isOpen"):
                        trackingCode = trackingCodes.pop()
                except:
                    pass
                
                netSuiteDetails = lineItem.get("item")
                parent = self.getParentNetsuiteProduct(netSuiteDetails.get("id"))
                
                if parent is None:
                    parent = {}
                    
                netSuiteItemType = lineItem.get("itemType",{}).get("refName")
                if netSuiteItemType not in ["NonInvtPart","InvtPart","Assembly","Service","OthCharge"]:
                    if netSuiteItemType=="Discount":
                        discounts.append({"amount":abs(lineItem.get("rate")),"item":netSuiteDetails.get("refName")})
                        continue
                    else:
                        print(netSuiteItemType)
                        print(json.dumps({x:lineItem[x] for x in filter(lambda y:not y.startswith("cust"),lineItem.keys())},indent=1))
                        sys.exit()
                
                    
                details = {
                    "sku":netSuiteDetails.get('refName','').split(":")[-1].strip(),
                    "quantity":self.lineItemQuantity(lineItem),#.get("quantity")),
                    "priceSet":{
                        "shopMoney":{
                            "currencyCode":"USD",
                            "amount":self.lineItemPrice(lineItem),#lineItem.get("rate"),
                        },
                    },
                    "title":self.lineItemTitle(lineItem,parent),
                    "requiresShipping":False if netSuiteItemType in ["Service","OthCharge"] else True,
                    "_netSuiteId":netSuiteDetails.get("id")
                }
                shopifyVariant = self.getVariantIdByExternalId(netSuiteDetails.get("id"))
                if shopifyVariant:
                    shopifyVariantId = shopifyVariant.get("shopifyGraphQLId")
                    details["sku"] = shopifyVariant.get("SKU")
                    details["variantId"] = shopifyVariantId
                    self.variantsFound[shopifyVariantId] = True
                    
                    #print(f"found variant {shopifyVariantId}")
                else:
                    #print(f"could not find variant {jpath('item.id',lineItem)}")
                    self.variantsNotFound[jpath('item.id',lineItem)] = True
                if trackingCode:
                    details["_trackingCode"] = trackingCode
                    
                details["_cancelled"]=lineItem.get("isClosed")
                details["_fulfilled"]= not lineItem.get("isOpen")
                details["_originalFulfilled"] = not lineItem.get("isOpen")
                
                if lineItem.get("rate",0)<0:
                    discounts.append({
                        "amount":abs(lineItem.get("rate")),
                        "item":details.get("title")
                    })
                elif lineItem.get("quantity") <1:
                    pass
                else:
                    order["lineItems"].append(details)
                
            if record.search("customFields.stc_tax_after_discount",0)>0:
                order["taxLines"] = [{
                    "title":"Tax Paid",
                    "rate":record.search("customFields.stc_tax_after_discount")
                }]
            if record.has("email"):
                order["email"] = record.get("email")
                
            if len(discounts)>0:
                order["_appliedDiscount"] = float(functools.reduce(lambda a,b:a+b.get("amount"),discounts,0))
                
            fulfilledCount = functools.reduce(lambda a,b:a+1 if b.get("_fulfilled") else a+0,order["lineItems"],0)
            cancelCount = functools.reduce(lambda a,b:a+1 if b.get("_cancelled") else a+0,order["lineItems"],0)
            
            if fulfilledCount==len(order.get("lineItems")):
                order["_orderFulfillmentStatus"] = "Fulfilled"
            elif fulfilledCount>0:
                order["_orderFulfillmentStatus"] = "Partially Fulfilled"
            else:
                if cancelCount==len(order.get("lineItems")):
                    order["_orderFulfillmentStatus"] = "Cancelled"
                else:
                    order["_orderFulfillmentStatus"] = "Pending Fulfillment"
            
            return order
            
