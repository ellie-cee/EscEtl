import json
from ..netsuiteClient import *
from etl.graphQL import Customer,Companies,MetaField
from etl.misc import *
from shopify_uploader import ShopifyUploader
import urllib.parse

class CustomerCreator(CustomerRecordAwareClient,AddressAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
    def run(self):
        gql = Customer(searchable=True)
        record: NetSuiteRecord
        shopifyId = None
        for record in self.consolidatedRecordList():
            print(record.recordId)
            if record.get("shopifyId"):
                continue
            input = {
                "input":{
                    
                    "email":record.email,
                    "note":f"NetSuite ID {record.get('netSuiteId')}",
                }
            }
            if is_phone(record.get("phone")):
                input["input"]["phone"] = format_phone(record.get("phone"))
            ret = gql.create(input)
            
            if ret.search("data.customerCreate.customer.id"):
                print("created Company")
                shopifyId = ret.search("data.customerCreate.customer.id")
                record.set("shopifyId",shopifyId)
                record.write()
            else:
                errorCode = ret.search("data.customerCreate.userErrors[0].message")
                if errorCode is not None:
                    if "TAKEN" in errorCode:
                        candidate = gql.find(record.email)
                        if (candidate):
                            record.set("shopifyId",candidate.get("id"))
                            record.write()
                else:    
                    print(json.dumps(ret.data,indent=1))
            
class CompanyCreator(CustomerRecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
    def flunk(self,customer,error,code="FIX",retryable=True):
        
        customer.set("_errorCode",code)
        customer.set("_creationError",error)
        if not retryable:
            customer.set("_noRetry",True)
        customer.write()
    def unflunk(self,customer):
        customer.set("_previousErrorCode",customer.get("_errorCode"))
        customer.delete("_errorCode")
        customer.delete("_creationError")
        customer.write()
    def getExternalId(self,record):
        for field in ["externalId","netSuiteId","netSuiteid","_externalId","_netSuiteId"]:
            if record.get(field):
                return record.get(field)
    def loadOrders(self):
        orders = {}
        ns = NetSuiteClient()
        for recordId in ns.recordList("salesOrder"):
            order = ns.loadRecord(recordId,type="salesOrder",searchable=True)
            custId = order.search("entity.id")
            if custId is not None:
                if custId in orders:
                    orders[custId].append(order.id)
                else:
                    orders[custId] = [order.id]
        self.orders = orders
        return orders        
    def run(self):
        self.loadOrders()
        gql = Companies(searchable=True)
        record: NetSuiteRecord
        shopifyId = None
        defaultRoleId=None
        for record in self.consolidatedRecordList():
            if self.has("onlyThese"):
                if len(self.onlyThese)>0 and record.recordId not in self.onlyThese:
                    continue
            if record.get("shopifyId"):
                
                continue
            if record.get("companyLocation") is None:
                self.flunk(record,"No Company Location",code="COMP")
                continue
            if record.recordId not in self.orders:
                print(f"customer {record.recordId} has no orders!")
                continue
            
            record.delete("_retryFlag")
            record.delete("_failureDetails")
            record.set("_errors",[])
            
            
            
            externalId = self.getExternalId(record)
            input = {
                "input":{
                    "company":{
                        "name":record.get("name"),
                        "customerSince":record.get("customerSince"),
                        "externalId":externalId,
                        "note":"imported from netsuite"
                    },
                    "companyLocation":stripShopify(record.get("companyLocation")),
                }
            }
            ret = gql.createCompany(input)
            
            shopifyId = ret.search("data.companyCreate.company.id")
            if not shopifyId:
                if ret.hasErrors():
                    print(json.dumps([ret.errors(), input,ret.data],indent=1))
                    if ret.hasErrorCode("TAKEN"):
                        shopifyDetails:GqlReturn
                        shopifyDetails = gql.getByExternalId(externalId)
                        if shopifyDetails is not None:
                            
                            print(f"Found record for {externalId}")
                            
                            shopifyId = shopifyDetails.search("id")
                            record.set("shopifyId",shopifyId)
                            
                            defaultRoleId = shopifyDetails.search("defaultRole.id")
                            record.set("companyLocation.shopifyId",shopifyDetails.search("data.companies.nodes[0].locations.nodes[0].id"))
                            record.set("errors",[])
                            record.write()
                        else:
                            record.set("_creationError",shopifyDetails.errorMessages())
                            record.set("retryFlag",True)
                            record.write()
                            print(f"Unable to create company for {record.get('externalId')}")
                            continue
                    else:
                        record.set("_creationError",shopifyDetails.errorMessages())
                        record.set("retryFlag",True)
                        record.write()
                        print(f"Unable to create company for {record.get('externalId')}")
                        continue
            else:
                print("Company created")
                shopifyId = ret.search("data.companyCreate.company.id")
                record.set("shopifyId",shopifyId)
                defaultRoleId = ret.search("data.companyCreate.company.defaultRole.id")
                record.set("companyLocation.shopifyId",ret.search("data.companyCreate.company.locations.nodes[0].id"))
                record.set("errors",[])
            
            for location in record.get("locations"):
                
                ret = gql.addLocation(
                    {
                        "companyId":record.get("shopifyId"),
                        "input":stripShopify(location),
                    }
                )
                if not ret.get("data"):
                    print(json.dumps(ret.data,indent=1))
                else:
                    location["shopifyId"] = ret.search("data.companyLocationCreate.companyLocation.id")
                    print(f"Created Location {location.get('name')}")
            mainAssigned = False
            for contact in record.get("contacts"):
                
                ret = gql.addContact(
                    {
                        "companyId":record.get("shopifyId"),
                        "input":stripShopify(contact),
                    }
                )
                if ret.hasErrors():
                    if ret.hasErrorCode("TAKEN"):
                        ret = gql.findAndAssignContact(record.get("shopifyId"),contact.get("email"))
                        if ret is None:
                            record.set("_creationError",", ".join(ret.errorMessages()))
                        contact["shopifyId"] = ret.get("companyContactId")
                        contact["shopifyCustomerId"] = ret.get("customerId")
                    else:
                        print(json.dumps(ret.errors()))
                        continue
                else:
                    contact["shopifyId"] = ret.search("data.companyContactCreate.companyContact.id")
                    contact["shopifyCustomerId"] = ret.search("data.companyContactCreate.companyContact.customer.id")
                
                if contact.get("shopifyId"):
                    print(f"Contact {contact.get('email')} created")
                    if not mainAssigned:
                        print(f"Contact {contact.get('email')} is main contact")
                        assignedContact = gql.assignMainContact({"companyId":record.get("shopifyId"),"companyContactId":contact.get("shopifyId")})
                        mainAssigned = True
                    
                    for location in record.get("locations")+[record.get("companyLocation")]:
                        assigned =  gql.assignContactToLocation(
                                location.get("shopifyId"),
                                contact.get("shopifyId"),
                                defaultRoleId
                            )
                        print(f"Locations for {contact.get('email')} assigned")
            record.write()