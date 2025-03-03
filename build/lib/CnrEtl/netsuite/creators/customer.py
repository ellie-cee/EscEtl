import json
import subprocess
from ..netsuiteClient import *
from ..consolidators import *
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
                            self.postProcess(record)
                else:    
                    print(json.dumps(ret.data,indent=1))
            
class CompanyCreator(CustomerRecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        self.shopifyInit()
    def orderTypes(self):
        return ["cashSale"]
    def loadCustomerOrders(self):
        if hasattr(self,"orderEntityIds"):
            return self.orderEntityIds
        
        entityIds = []
        for orderType in self.orderTypes():
            for orderId in self.recordList(orderType):
                record = self.loadSupportingRecord(orderId,"cashSale")
                orderEntityId = record.search("entity.id")
                if orderEntityId not in entityIds:
                    entityIds.append(orderEntityId)
        setattr(self,"orderEntityIds",entityIds)
        return entityIds
    def hasOrders(self,company):
        entityId = company.get("_externalId")
        return entityId in self.loadCustomerOrders()
    
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
        
        gql = Companies(searchable=True)
        metafieldGQL = MetaField()
        record: NetSuiteRecord
        shopifyId = None
        defaultRoleId=None
        for record in self.consolidatedRecordList():
            if hasattr(self,"only") and len(self.only)>0 and record.recordId not in self.only:
                continue       
            if record.get("shopifyId"):
                
                continue
            if record.get("companyLocation") is None:
                self.flunk(record,"No Company Location",code="COMP")
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
            
            companyCreated = gql.createCompany(input)
            
            
            companyLocationId = companyCreated.search("data.companyCreate.company.locations.nodes[0].id")
            
            record.set("companyLocation.shopifyId",companyLocationId)
            shopifyId = companyCreated.search("data.companyCreate.company.id")
            if not shopifyId:
                if companyCreated.hasErrors():
                    
                    if companyCreated.hasErrorCode("TAKEN"):
                        shopifyDetails:GqlReturn
                        shopifyDetails = gql.getByExternalId(externalId)
                        if shopifyDetails is not None:
                            companyLocationId = shopifyDetails.search("locations.nodes[0].id")
                            record.set("companyLocation.shopifyId",companyLocationId)
                            
                            
                            shopifyId = shopifyDetails.search("id")
                            record.set("shopifyId",shopifyId)
                            
                            defaultRoleId = shopifyDetails.search("defaultRole.id")
                            
                            record.set("errors",[])
                            record.write()
                        else:
                            record.set("_creationError",shopifyDetails.errorMessages())
                            record.set("retryFlag",True)
                            record.write()
                            
                            continue
                    else:
                        record.set("_creationError",companyCreated.errorMessages())
                        record.set("retryFlag",True)
                        record.write()
                        
                        
                        
                        continue
            else:
                
                ret = metafieldGQL.pushFields(
                    {
                        "metafields":[
                                {
                                    "namespace":"cnr",
                                    "key":"netsuite_id",
                                    "value":record.recordId,
                                    "type":"single_line_text_field",
                                    "ownerId":shopifyId
                                },
                                {
                                    "namespace":"cnr",
                                    "key":"tg_number",
                                    "value":record.get("_tgNumber"),
                                    "type":"single_line_text_field",
                                    "ownerId":shopifyId
                                },
                                {
                                    "namespace":"cnr",
                                    "key":"account_number",
                                    "value":record.get("_accountNumber"),
                                    "type":"single_line_text_field",
                                    "ownerId":shopifyId
                                }
                                
                        ]
                    }
                )
                
                shopifyId = companyCreated.search("data.companyCreate.company.id")
                record.set("shopifyId",shopifyId)
                record.set("_defaultRoleId",defaultRoleId)
                record.write()
                defaultRoleId = companyCreated.search("data.companyCreate.company.defaultRole.id")
                record.set("errors",[])
                
                
            self.createLocations(record)
            mainAssigned = False
            for contact in record.get("contacts"):
                
                ret = gql.addContact(
                    {
                        "companyId":record.get("shopifyId"),
                        "input":stripShopify(contact,extra=["phone"]),
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
                        
                        continue
                else:
                    contact["shopifyId"] = ret.search("data.companyContactCreate.companyContact.id")
                    contact["shopifyCustomerId"] = ret.search("data.companyContactCreate.companyContact.customer.id")
                if contact.get("shopifyId"):
                    
                    
                    self.assignContactToLocations(
                        contact,
                        record,
                        defaultRoleId
                    )
            mainContactEmail = record.get("_mainContact")
            if mainContactEmail is None:
                rawRecord = self.loadRecord(record.recordId)
                mainContactEmail = rawRecord.get("email")
            mainContact = next(filter(lambda x:x.get("email","")==mainContactEmail,record.get("contacts",[])),None)
            if mainContact is None:
                mainContact = next(filter(lambda x:x.get("shopifyId") is not None,record.get("contacts",[])),None)
            gql.setMainContact(
                {
                    "companyId":record.get("shopifyId"),
                    "companyContactId":mainContact.get("shopifyId")
                }
            )
                    
                    
                        
            record.write()
    def addContactFromSale(self,company:NetSuiteRecord,order:NetSuiteRecord):
        contact = {
            "email":order.get("email"),
            "firstName":None,
            "lastName":None
        }
        for addressType in ["billingAddress","shippingAddress"]:
            attn =  order.search(f"{addressType}.attention",None)
            if attn is not None:
                parts = attn.split(" ")
                contact["firstName"] = parts[0]
                contact["lastName"] = " ".join(parts[1:])
                break
        if contact["firstName"] is None:
            contactCount = len(company.get("contacts",[]))+1
            contact["firstName"] = "Company"
            contact["lastName"] = f"Contact {contactCount}"
            
        gql = Companies(searchable=True)
        ret = gql.addContact(
            {
                "companyId":company.get("shopifyId"),
                "input":contact,
            }
        )
        
        
        contact["shopifyId"] = ret.search("data.companyContactCreate.companyContact.id")
        contact["shopifyCustomerId"] = ret.search("data.companyContactCreate.companyContact.customer.id")
        if contact.get("shopifyId") is not None:
            
            self.assignContactToLocations(
                contact,
                company,
                company.get("_defaultRoleId")
            )
            company.append("contacts",contact)
            company.write()
            return contact
        return None
        
        
    def createLocations(self,company):
        gql = Companies(searchable=True)
        locationIds = []
        for location in company.get("locations"):
            if self.getAddressOrders(location)<1:
                continue
            ret = gql.addLocation(
                {
                    "companyId":company.get("shopifyId"),
                    "input":stripShopify(location),
                }
            )
            if ret.hasErrors():
                pass
            else:
                locationId = ret.search("data.companyLocationCreate.companyLocation.id")
                location["shopifyId"] = locationId
                locationIds.append(locationId)
        
                print(f"Created Location {location.get('name')}")
        locationIds.append(company.search("companyLocation.shopifyId"))
        gql.addLocationsToCatalog(company.get("shopifyCatalogId"),locationIds)
        company.write()
        
                
    def getAddressOrders(self,location):
        totalOrders = 0
        for dir in ["order","salesOrder"]:
            for addressType in ["billingAddress","shippingAddress"]:
                if location.get(addressType) is None:
                    continue
                orders = list(
                    filter(
                        lambda x:x!='',
                        subprocess.run(
                            [
                                "grep","-ir",SearchableDict(location).search(f"{addressType}.address1"),f"records/consolidated/{dir}"
                            ],
                            capture_output=True
                        ).stdout.decode("utf-8").split("\n")
                    )
                )
                totalOrders = totalOrders + len(orders)
        return totalOrders
    def getContactOrders(self,contact):
        totalOrders = 0
        for dir in ["order","salesOrder"]:
            orders = list(
                filter(
                    lambda x:x!='',
                    subprocess.run(
                        [
                            "grep","-ir",contact.get("email"),f"records/consolidated/{dir}"
                        ],
                        capture_output=True
                    ).stdout.decode("utf-8").split("\n")
                )
            )
            totalOrders = totalOrders + len(orders)
        return totalOrders
    def assignContactToLocations(self,contact,company,roleId):
        if self.getContactOrders(contact)<1:
            if len(company.get("contacts",[]))>=50:
                if isinstance(contact,NetSuiteRecord):
                    contact.set("_locations",[])
                else:
                    contact["_locations"] = []
                return
                
        
        gql = Companies(searchable=True)
        locations = []
        for location in company.get("locations")+[company.get("companyLocation")]:
            if location.get("shopifyId") is None:
                continue
            assigned =  gql.assignContactToLocation(
                location.get("shopifyId"),
                contact.get("shopifyId"),
                roleId
            )
            if not assigned.hasErrors():
                locations.append(location.get("shopifyId"))
            else:
               pass
        
        if isinstance(contact,NetSuiteRecord):
            contact.set("_locations",locations)
        else:
            contact["_locations"] = locations
        print(f"Locations for {contact.get('email')} assigned")