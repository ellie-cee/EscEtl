import functools
import json
from ..netsuiteClient import *
from etl.graphQL import Order,MetaField
from etl.misc import *
import csv

class OrderCreator(OrderRecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
        self.logger = csv.DictWriter(
            open("output/order-create-log.csv","w"),
            delimiter=',',
            quotechar='"',
            fieldnames=[
                "externalId",
                "status",
                "shopifyId",
                "failedData"
            ]
        )
        self.logger.writeheader()
    def getLineItemBySKU(self,sku,lineItems):
        for index,lineItem in enumerate(lineItems):
            if lineItem.get("sku")==sku:
                return lineItem,index
        return None
    def flunk(self,order,error,code="GEN",retryable=True):
        
        print(f"{order.recordId} Failed: {error}")
        order.set("_creationError",error)
        order.set("_code",code)
        if not retryable:
            order.set("_noRetry",False)
        order.write()
    def run(self):
        
        orderGQL = Order(searchable=True)
        metafieldGQL = MetaField(searchable=True)
        for order in self.consolidatedRecordList():
            if self.has("only") and len(self.only)>0:
                if not order.recordId in self.only:
                    continue
            if len(order.get("lineItems",[]))<1:
                self.flunk(order,"No line Items",code="NLI",retryable=False)
                
                continue
         
            if order.get("shopifyId"):
                continue
            if order.get("_noRetry"):
                print(f"{order.recordId} not retryable")
                continue
            
                
            if order.get("status")=="Cancelled":
                self.flunk(order,"Order Cancelled",code="CANC",retryable=False)
                continue
            if False:
                company = order.search("purchasingEntity.purchasingCompany",{})
                validCompany = True
                for field in ["companyId","companyContactId","companyLocationId"]:
                    if not company.get(field):
                        validCompany = False
                        break
                if not validCompany:
                    self.flunk(order,"Invalid Company Information",code="COMP")
                    continue
            
                
            ignoreFields = ["name","createdAt","status"]
            
            orderDetails = {x:order.get(x) for x in filter(lambda x:x not in ignoreFields,list(order.data.keys()))}
            
            
               
            #orderDetails["email"] = "fattycrabcakes@gmail.com"
            if "email" in orderDetails:
                del orderDetails["email"]
                
            discounts = []
            finalLineItems = []

                
            
                
            for lineItem in orderDetails["lineItems"]:
                amount = SearchableDict(lineItem).search("priceSet.shopMoney.amount")
                if amount is None:
                    continue
                if lineItem.get("quantity")<1:
                    continue
                if amount<0:
                    discounts.append({
                        "amount":abs(amount),
                        "item":lineItem.get("title")
                    })
                else:
                    if lineItem.get("title") is None:
                        lineItem["title"] = "item"
                    if len(lineItem["title"])>255:
                            lineItem["title"] = f'{lineItem["title"][0:252]}...'
                            
                    finalLineItems.append(lineItem)
                    
            if not next(filter(lambda x:x["key"]=="original_order_date",orderDetails["metafields"]),None):
                orderDetails["metafields"].append({
                    "key":"original_order_date",
                    "namespace":"cnr",
                    "type":"date_time",
                    "value":order.get("processedAt")
                })
            orderDetails["lineItems"] = finalLineItems
            if len(finalLineItems)<1:
                self.flunk(order,"No valid Line Items",code="NLI",retryable=False)
                continue
            
            orderInput = {
                "options":{
                    "inventoryBehaviour":"BYPASS",
                    "sendFulfillmentReceipt":False,
                    "sendReceipt":False,
                },
                "order":orderDetails
            }
            
            draft = orderGQL.createOrder(self.privatize(orderInput))
            
            orderId = draft.search("data.orderCreate.order.id")
            if orderDetails.get("_appliedDiscount",0)>0:
                orderGQL.addDiscount(orderId,orderDetails.get("_appliedDiscount"))
            
            
                
            if orderId:
                if "Pending Billing" not in order.get("_status"):
                    orderGQL.markasPaid(orderId)
                order.set("shopifyGraphQLId",orderId)
                order.set("shopifyId",orderId.split("/")[-1])
                order.write()
                
                try:
                    for fulfillmentOrder in map(lambda x:SearchableDict(x),draft.search("data.orderCreate.order.fulfillmentOrders.nodes",[])):
                        fulfillmentOrderId = fulfillmentOrder.get("id")
                            
                        fulfillmentOrderTrackingCodes = []
                        itemsToFulfill = []
                        for lineItem in fulfillmentOrder.search("lineItems.nodes",[]):
                            ourLineItem,index = self.getLineItemBySKU(lineItem.get("sku"),order.get("lineItems"))
                            if ourLineItem.get("_fulfilled"):
                              
                                order.data["lineItems"][index]["_fulfilled"] = True
                                if ourLineItem.get("_trackingCode"):
                                    fulfillmentOrderTrackingCodes.append(ourLineItem.get("_trackingCode"))
                                itemsToFulfill.append(
                                    {
                                        "id":lineItem.get("id"),
                                        "quantity":lineItem.get("quantity")
                                    }
                                )
                                
                            
                        if len(itemsToFulfill)>0:
                            fulfillmentsInput = {
                                "fulfillment": {
                                    "lineItemsByFulfillmentOrder": {
                                        "fulfillmentOrderId": fulfillmentOrderId,
                                        "fulfillmentOrderLineItems":itemsToFulfill,
                                    },
                                    "notifyCustomer":False,
                                    "trackingInfo":{
                                        "numbers":fulfillmentOrderTrackingCodes
                                    }
                                }
                            }
                                
                            fulfillments = orderGQL.fulfilItems(fulfillmentsInput)
                            
                            if fulfillments.hasErrors():
                                logJSON(fulfillments.errorMessages())
                            order.write()
                            print(f"Created Order {order.recordId}")
                                
                        
                        
                        for metafield in orderDetails["metafields"]:
                            metafield["ownerId"] = orderId
                        metafieldGQL.pushFields({"metafields":[x|{"ownerId":orderId} for x in orderDetails["metafields"]]})
                        
                except SystemExit:
                    pass
                except:
                    traceback.print_exc()
                    self.logger.writerow({
                        "externalId":order.recordId,
                        "status":"FAILED",
                        "failedData":traceback.format_exc(),
                        "shopifyId":""
                    })
                    orderGQL.delete(orderId)
                    order.delete("shopifyId")
                    order.delete("shopifyGraphQLId")
                    for lineItem in order.data["lineItems"]:
                        lineItem["_fulfilled"] = lineItem["_originalFulfilled"]
                    order.write()
                        
            else:
               
                error = draft.search("data.orderCreate.userErrors[0].message")
                if error is None:
                    
                    error = f"Entity ID  {order.get('_entityId')}"+"; ".join(list(map(lambda x:x.get("message"),draft.search("data.draftOrderCreate.userErrors",[]))))
                self.logger.writerow(
                    {
                        "externalId":order.recordId,
                        "shopifyId":"",
                        "status":"FAILED",
                        "failedData":error
                    }
                )
                self.flunk(order,error,code="FIX")
                
                
             
                    

