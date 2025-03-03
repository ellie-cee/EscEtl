from .base import *

class Order(GraphQL):
    def __init__(self, debug=False, searchable=False):
        super().__init__(debug, searchable)
    def all(self):
        return self.iterable(
            """
            query getOrders ($after:String) {
                orders(first:250,after:$after) {
                    nodes {
                        id
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {"after":None},
            dataroot="data.orders"
        )
    def get(self,orderId):
        return self.run(
            """
            query getorder($id:ID!) {
                order(id:$id) {
                    id
                }
            }
            """,
            {
                "id":orderId
            }
        )
    def createOrder(self,input):
        return self.run(
            """
mutation OrderCreate($order: OrderCreateOrderInput!,$options: OrderCreateOptionsInput) {
  orderCreate(order: $order, options: $options) {
    userErrors {
      field
      code
      message
    }
    order {
      id
      totalTaxSet {
        shopMoney {
          amount
          currencyCode
        }
      }
      lineItems(first: 5) {
        nodes {
          variant {
            id
          }
          id
        }
      }
      fulfillmentOrders(first: 50) {
        nodes {
          id
          lineItems(first: 100) {
            nodes {
              id
              title: variantTitle
              sku
              quantity: totalQuantity
            }
          }
        }
      }
    }
  }
}

""",
            input
        )
    def markasPaid(self,id):
        return self.run(
            """
            mutation orderMarkAsPaid($input: OrderMarkAsPaidInput!) {
                orderMarkAsPaid(input: $input) {
                     order {
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
                "input": {
                    "id":id
                }
            }
        )
    def createDraft(self,input):
        return self.run(
            """
            mutation draftOrderCreate($input: DraftOrderInput!) {
                draftOrderCreate(input: $input) {
                    draftOrder {
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
    def closeDraft(self,id):
        return self.run(
            """
            mutation draftOrderComplete($id: ID!) {
                draftOrderComplete(id: $id) {
                    draftOrder {
                        id
                        order {
                            id
                            lineItems(first:100) {
                                nodes {
                                    id
                                    sku
                                }
                            }
                            fulfillmentOrders(first:50) {
                                nodes {
                                    id
                                    lineItems(first:100) {
                                        nodes {
                                            id
                                            title: variantTitle
                                            sku
                                            quantity: totalQuantity
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """,
            {
                "id":id
                    
            }
        )
    def delete(self,id):
        return self.run(
            """
                mutation orderDelete($orderId: ID!) {
                    orderDelete(orderId: $orderId) {
                        deletedId
                        userErrors {
                            field
                            message
                    }
                }
            }
            """,
            {"orderId":id}
        )
    
    def fulfilItems(self,input):
        
        return self.run(
            """
            mutation fulfillmentCreateV2($fulfillment: FulfillmentV2Input!) {
                fulfillmentCreateV2(fulfillment: $fulfillment) {
                    fulfillment {
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
                
    def getFulFillmentOrders(self,id):    
        return self.run(
            """
            query getOrderWithFullfillments($id:ID!) {
                order(id:$id) {
                    id
                    lineItems(first:100) {
                        nodes {
                            id
                            variant {
                                id
                                sku
                            } 
                        }
                    }
                    fulfillmentOrders(first:20) {
                        nodes {
                            id
                            lineItems(first:100) {
                                nodes {
                                    id
                                    variantTitle
                                    sku
                                    totalQuantity
                                }
                            }
                        }    
                    }            
                }
            }""",
            {"id":f"gid://shopify/Order/{id}"}
        )
        
    def orderEditBegin(self,id):
        return self.run(
            """
                mutation orderEditBegin($id: ID!) {
                    orderEditBegin(id: $id) {
                        calculatedOrder {
                            id
                            lineItems(first:100) {
                                id
                                quantity
                                originalUnitPriceSet {
                                    presentmentMoney {
                                        amount
                                    }
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
            {"id":id}
        )
    def orderEditClose(self,id):
        return self.run(
            """
            mutation orderEditCommit($id: ID!,$staffNote:String) {
                orderEditCommit(id: $id,staffNote:$staffNote) {
                    order {
                        id
                        currentSubtotalPriceSet {
                            presentmentMoney {
                                amount
                            }
                            shopMoney {
                                amount
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
                "id":id,
                "notifyCustomer": False,
                "staffNote": "Added Bundle Items"
            }
        )
    def orderEditAddItem(self,id,variant,quantity):
        return self.run(
            """
            mutation orderEditAddVariant($id: ID!, $quantity: Int!, $variantId: ID!) {
                orderEditAddVariant(id: $id, quantity: $quantity, variantId: $variantId) {
                    calculatedLineItem {
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
                "allowDuplicates": True,
                "id": id,
                "quantity": quantity,
                "variantId": f"gid://shopify/ProductVariant/{variant}"
            }
            
        )
    def orderItemDiscount(self,id,line_item,discountAmount):
        return self.run(
            """
            mutation orderEditAddLineItemDiscount($discount: OrderEditAppliedDiscountInput!, $id: ID!, $lineItemId: ID!) {
                orderEditAddLineItemDiscount(discount: $discount, id: $id, lineItemId: $lineItemId) {
                    addedDiscountStagedChange {
                        id
                    }
                    calculatedLineItem {
                        id
                    }
                    userErrors {
                        field
                        message
                    }
                }
            }""",
            {
                "discount": {
                    "description": "Order discount",
                    "fixedValue": discountAmount
                },
                "id": id,
                "lineItemId": line_item
            }
        )
    def addDiscount(self,orderId,discountAmount):
        if discountAmount<=0:
            return
        
        orderEdit = SearchableDict(self.orderEditBegin(orderId).search("data.orderEditBegin.calculatedOrder",{}))
        orderEditId = orderEdit.get("id")
        if orderEditId is None:
            return None
        
        for lineItem in orderEdit.search("lineItems.nodes",[]):
            if discountAmount<=0:
                break
            lineTotal = jpath("originalUnitPriceSet.presentmentMoney.amount")* lineItem.get("quantity")
            
            if lineTotal>discountAmount:
                self.orderItemDiscount(orderEditId,lineItem.get("id",discountAmount))
                discountAmount = 0
            else:
                self.orderItemDiscount(orderEditId,lineItem.get("id",lineTotal))
                discountAmount = discountAmount - lineTotal
        
        self.orderEditClose(orderEditId)
    def getDrafts(self):
        return self.iterable(
            """
            query getDraftOrders($after:String) {
                draftOrders(first: 100,after:$after) {
                    nodes {
                        id
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
            """,
            {"after":None},
            dataroot="data.draftOrders"
        )
    def deleteDraft(self,draftId):
        return self.run(
            """
            mutation draftOrderDelete($input: DraftOrderDeleteInput!) {
                draftOrderDelete(input: $input) {
                    deletedId
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {
                "input":{
                    "id":draftId
                }
            }
        )
    def deleteDrafts(self,draftIds):
        print(draftIds)
        return self.run(
            """
            mutation draftOrderBulkDelete($ids:[ID!]!) {
                draftOrderBulkDelete(ids:$ids) {
                    job {
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
                "ids":draftIds
            }
        )
    def getCustomerOrders(self,customerId):
        return self.iterable(
            """
            query getrders($query:String,$after:String) {
                orders(first:20,after:$after,query:$query) {
                    nodes {
                        id
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            } 
            """,
            {"after":None,"query":f"customer_id:{customerId.split('/')[-1]}"}
        )
                
    