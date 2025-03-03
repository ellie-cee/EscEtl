from etl.graphQL import *
import json
from ..netsuiteClient import *
from slugify import slugify
from etl.misc import *
import functools
from ..creators import CompanyCreator



class OrderConsolidator(OrderRecordAwareClient,AddressAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()

        
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
    def hasUnfilledItems(self,record):
        return len(list(filter(lambda x:not x.get("_fulfilled"),record.get("lineItems"))))>0
    
    def run(self):
        self.productRef = json.load(open("records/consolidated/product/tree.json"))
        self.variantsFound = {}
        self.variantsNotFound = {}
        
        for recordId in self.recordList():
            if hasattr(self,"only") and len(self.only)>0 and recordId not in self.only:
                continue       
            
            record = self.loadRecord(recordId)
            
            if record is None:
                print(" no recordId")
                continue
            try:
                existingRecord = self.loadConsolidateRecord(record.recordId)
                order = self.processOrder(record,existingRecord)
                if order is None:
                    continue
                
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
                    self.postProcess(self.loadConsolidateRecord(recordId))
            except SystemExit:
                sys.exit()
                pass
            except KeyboardInterrupt:
                sys.exit()
            except:
                traceback.print_exc()
                sys.exit()
        
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
                
    def processOrder(self,incomingRecord:NetSuiteRecord,existingRecord:NetSuiteRecord):
            
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
                for location in company.get("locations"):
                    if location.get("shopifyId") is None:
                        continue
                    for addressType in ["billingAddress","shippingAddress"]:
                        if location.get(addressType) is not None:
                            address = location.get(addressType)
                            if address.get("address1") == order[addressType].get("address1"):
                                order["companyLocationId"] = location.get("shopifyId")
                                break                
                if company.get("companyLocationId") is None:
                    order["companyLocationId"] = company.search("companyLocation.shopifyId")
                
                contact = None
                for candidate in company.get("contacts",[]):
                    if candidate.get("email","").lower() == record.get("email","").lower() and candidate.get("shopifyCustomerId") is not None:
                        contact = candidate
                       
                        break
                
                if contact is None:
                    contact = next(filter(lambda x:x.get("email","").lower()==record.get("email","").lower() and x.get("shopifyCustomerId") is not None,company.get("contacts")),None)
                    if contact is None:
                        
                        shopifyCustomer = Customer().find(record.get("email"))
                        if shopifyCustomer is not None:
                            shopifyCustomer
                            customerId = shopifyCustomer.get("id")
                        
                        companyContactId = None
                        contactLocations = []
                        defaultRole = None
                        if customerId is not None:
                            for contactProfile in shopifyCustomer.search("companyContactProfiles",[]):
                                companyId = jpath("company.id",contactProfile)
                                if companyId == company.get("shopifyId"):
                                    defaultRole = jpath("company.defaultRole.id",contactProfile)
                                    companyContactId = contactProfile.get("id")
                                    contactLocations = list(
                                        map(
                                            lambda x:jpath("companyLocation.id",x),
                                            jpath("roleAssignments.nodes",contactProfile)
                                        )
                                    )
                                    break
                        if companyContactId is not None:
                            
                            newContact = {
                                    "firstName":shopifyCustomer.get("firstName"),
                                    "lastName":shopifyCustomer.get("lastName"),
                                    "email":shopifyCustomer.get("email"),
                                    "shopifyCustomerId":shopifyCustomer.get("id"),
                                    "shopifyId":companyContactId
                                }
                            company.append(
                                "contacts",
                                newContact
                            )
                            company.write()
                            order["customerId"] = customerId
                            if len(contactLocations)<1:
                                CompanyCreator().assignContactToLocations(
                                    newContact,
                                    company,
                                    defaultRole
                                )
                            
                        else:
                            contact = CompanyCreator().addContactFromSale(
                                company,
                                record
                            )
                            existingContact = None
                            if contact is not None:
                                
                                order["customerId"] = contact.get("shopifyCustomerId")
                                existingContact = next(
                                    filter(
                                        lambda x:x.get("email","").lower()==contact.get("email").lower(),
                                        company.get("contacts",[])
                                    ),
                                    None
                                )
                            if existingContact is not None:
                                company.append("contacts",contact)
                                company.write()
                            else:
                                contact = next(filter(lambda x:x.get("shopifyId") is not None,company.get("contacts",[])),None)
                                if contact is None:
                                    try:
                                        order["email"] = contact.get("email")
                                        order["customerId"] = contact.get("shopifyCustomerId")
                                    except:
                                        print(record.recordId)
                                        sys.exit()
                    else:   
                        
                        order["customerId"] = contact.get("shopifyCustomerId")
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
            subtotal = 0
            for index,lineItem in enumerate(record.get("item")):
                netSuiteDetails = lineItem.get("item")
                parent = self.getParentNetsuiteProduct(netSuiteDetails.get("id"))
                if existingRecord is not None:
                    existingLineItem = existingRecord.search(f"lineItems[{index}]")
                    if existingLineItem is not None:
                        
                        if existingLineItem.get("_fulfilled") and False:
                            existingLineItem["title"] = lineItem.get("itemDescription"),
                            order["lineItems"].append(existingLineItem)
                            continue
                
                trackingCode = None
                if lineItem.get("isClosed"):
                    continue
                
                try:
                    if not lineItem.get("isOpen"):
                        trackingCode = trackingCodes.pop()
                except:
                    pass
                
                                
                netSuiteItemType = lineItem.get("itemType",{}).get("refName")
                if netSuiteItemType not in ["NonInvtPart","InvtPart","Assembly","Service","OthCharge"]:
                    if netSuiteItemType=="Discount":
                        discounts.append({"amount":abs(lineItem.get("amount")),"item":netSuiteDetails.get("refName")})
                        continue
                    else:
                        
                        
                        sys.exit()
                try:
                    subtotal = subtotal + (self.lineItemPrice(lineItem)*self.lineItemQuantity(lineItem))
                except:
                    pass
                details = {
                    "sku":netSuiteDetails.get('refName','').split(":")[-1].strip(),
                    "quantity":self.lineItemQuantity(lineItem),#.get("quantity")),
                    "priceSet":{
                        "shopMoney":{
                            "currencyCode":"USD",
                            "amount":self.lineItemPrice(lineItem),#lineItem.get("rate"),
                        },
                    },
                    "title":self.lineItemTitle(lineItem,parent) if parent is not None else lineItem.get("description",f"SKU {netSuiteDetails.get('refName')}"),
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
                        "amount":abs(lineItem.get("amount")),
                        "item":details.get("title")
                    })
                elif lineItem.get("quantity") <1:
                    pass
                else:
                    order["lineItems"].append(details)
            if len(discounts)>0:
                order["_appliedDiscount"] = float(functools.reduce(lambda a,b:a+b.get("amount"),discounts,0))    
            else:
                order["_appliedDiscount"] = 0
                
            if record.search("customFields.stc_tax_after_discount",0)>0:
                taxAmount = record.search("customFields.stc_tax_after_discount")
                appliedDiscounts = order["_appliedDiscount"]
                
                order["taxLines"] = [{
                    "priceSet":{
                        "shopMoney":{
                            "amount":taxAmount,
                            "currencyCode":"USD"
                        }
                    },
                    "title":"Tax Paid",
                    "rate":(taxAmount/(subtotal-appliedDiscounts))
                }]
                
            if record.has("email"):
                order["email"] = record.get("email")
                
            
                
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
            
