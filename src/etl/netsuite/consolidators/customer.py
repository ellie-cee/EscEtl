from etl.graphQL import *
import json
from ..netsuiteClient import *
from slugify import slugify
from etl.misc import *


class CustomerConsolidator(CustomerRecordAwareClient,AddressAwareClient):
    tax_exemptions = ["CA_BC_COMMERCIAL_FISHERY_EXEMPTION","CA_BC_CONTRACTOR_EXEMPTION","CA_BC_PRODUCTION_AND_MACHINERY_EXEMPTION","CA_BC_RESELLER_EXEMPTION","CA_BC_SUB_CONTRACTOR_EXEMPTION","CA_DIPLOMAT_EXEMPTION","CA_MB_COMMERCIAL_FISHERY_EXEMPTION","CA_MB_FARMER_EXEMPTION","CA_MB_RESELLER_EXEMPTION","CA_NS_COMMERCIAL_FISHERY_EXEMPTION","CA_NS_FARMER_EXEMPTION","CA_ON_PURCHASE_EXEMPTION","CA_PE_COMMERCIAL_FISHERY_EXEMPTION","CA_SK_COMMERCIAL_FISHERY_EXEMPTION","CA_SK_CONTRACTOR_EXEMPTION","CA_SK_FARMER_EXEMPTION","CA_SK_PRODUCTION_AND_MACHINERY_EXEMPTION","CA_SK_RESELLER_EXEMPTION","CA_SK_SUB_CONTRACTOR_EXEMPTION","CA_STATUS_CARD_EXEMPTION","EU_REVERSE_CHARGE_EXEMPTION_RULE","US_AK_RESELLER_EXEMPTION","US_AL_RESELLER_EXEMPTION","US_AR_RESELLER_EXEMPTION","US_AZ_RESELLER_EXEMPTION","US_CA_RESELLER_EXEMPTION","US_CO_RESELLER_EXEMPTION","US_CT_RESELLER_EXEMPTION","US_DC_RESELLER_EXEMPTION","US_DE_RESELLER_EXEMPTION","US_FL_RESELLER_EXEMPTION","US_GA_RESELLER_EXEMPTION","US_HI_RESELLER_EXEMPTION","US_IA_RESELLER_EXEMPTION","US_ID_RESELLER_EXEMPTION","US_IL_RESELLER_EXEMPTION","US_IN_RESELLER_EXEMPTION","US_KS_RESELLER_EXEMPTION","US_KY_RESELLER_EXEMPTION","US_LA_RESELLER_EXEMPTION","US_MA_RESELLER_EXEMPTION","US_MD_RESELLER_EXEMPTION","US_ME_RESELLER_EXEMPTION","US_MI_RESELLER_EXEMPTION","US_MN_RESELLER_EXEMPTION","US_MO_RESELLER_EXEMPTION","US_MS_RESELLER_EXEMPTION","US_MT_RESELLER_EXEMPTION","US_NC_RESELLER_EXEMPTION","US_ND_RESELLER_EXEMPTION","US_NE_RESELLER_EXEMPTION","US_NH_RESELLER_EXEMPTION","US_NJ_RESELLER_EXEMPTION","US_NM_RESELLER_EXEMPTION","US_NV_RESELLER_EXEMPTION","US_NY_RESELLER_EXEMPTION","US_OH_RESELLER_EXEMPTION","US_OK_RESELLER_EXEMPTION","US_OR_RESELLER_EXEMPTION","US_PA_RESELLER_EXEMPTION","US_RI_RESELLER_EXEMPTION","US_SC_RESELLER_EXEMPTION","US_SD_RESELLER_EXEMPTION","US_TN_RESELLER_EXEMPTION","US_TX_RESELLER_EXEMPTION","US_UT_RESELLER_EXEMPTION","US_VA_RESELLER_EXEMPTION","US_VT_RESELLER_EXEMPTION","US_WA_RESELLER_EXEMPTION","US_WI_RESELLER_EXEMPTION","US_WV_RESELLER_EXEMPTION","US_WY_RESELLER_EXEMPTION"]
    onlyTheseIds = []
    def __init__(self,**kwargs):
        super().__init__(**kwargs)
        
    def meetsAddressMinimum(self,address):
        ret = True
        for field in ["address1","city","zip","zoneCode"]:
            if address.get(field,"") is None or address.get(field,"")=="":
                ret = False          
        return ret
        

    def addressHandle(self,address):
        try:
            return  slugify(" ".join([address.get(x,"") if address.get(x) is not None else "" for x in ["address1","city","state","zoneCode","countryCode"]]))
        except:
            
            print(json.dumps(address,indent=1))
            sys.exit()
    
    def ignoreRecipients(self):
        return []
    def isIgnoredRecipient(self,recipient):
        for ignored in self.ignoreRecipients():
            if recipient.startswith(ignored):
                return True
        return False
        
    def mapAddresses(self,customer,justBilling=False,exclude=None):
        ret = []
        customFields = customer.get("customFields")
        dupcheck = []
        detected = []
        for address in customer.get("addressBook",[]):
            rawAddress = address.get("addressBookAddress")
            rawAddress["id"] = address.get("id")
            details = self.mapAddress(rawAddress)
            
            if details  is None:
                print("no details!")
                continue
            details["billing"]=address.get("defaultBilling")
            details["shipping"]=address.get("defaultBilling")
            
                
                
                
                
                    
        
            
            asig = self.addressHandle(details)
            if asig in dupcheck:
                print(f"dup: {asig} {customer.get('id')}")
                continue
            dupcheck.append(asig)
            
            if customFields.get("cwgp_is_sales_tax_exempt"):
                details["taxExemptions"] = list(filter(lambda y:y.startswith(f"{details.get('countryCode')}_{details.get('zoneCode')}".upper()),self.tax_exemptions))
            if exclude is None or details.get("_externalId")!=exclude:
                ret.append(details)   
        return ret
    def contactKey(self,contact):
        
        return slugify("|".join(
            [contact.get(x) for x in filter(lambda y:contact.get(y) is not None,["firstName","lastName","phone"])]           
        ))
    def getDefaultEmail(self,customer):
        return customer.get("email")
    def mapContacts(self,customer,existingCustomer):
        ret = []
        customFields = customer.get("customFields")
        emails = []
        existingContacts = {}
        defaultEmail = self.getDefaultEmail(customer)

        
        if existingCustomer is not None:
            for existingContact in existingCustomer.get("contacts",[]):
                if existingContact is not None:
                    if existingContact.get("shopifyId"):
                        existingContacts[self.contactKey(existingContact)] = existingContact
                        
        
        for contact in reversed(customer.get("contactRoles")):

           
            name = contact.get("contactName")
            details = {
                "email":contact.get("email",defaultEmail),
                "firstName":name.split(" ")[0],
                "lastName":name.split(" ")[-1],
                "phone":customer.get("phone",""),
                #"externalid":contact.get("contact").get("id"),
            }
            
            if self.contactKey(details) in existingContacts:
                existingContact = existingContacts[self.contactKey(details)]
                if existingContact is not None:
                    details["shopifyId"] = existingContact.get("shopifyId")
                    details["shopifyCustomerId"] = existingContact.get("shopifyCustomerId")
            if details.get("firstName")!="Corporate" and details.get("email") not in emails and details.get("email") is not None:
                emails.append(details.get("email"))
                ret.append(details)
        
        return ret    

    def addressByType(self,addresses,key):
        for address in addresses:
            if address.get(key):
                return address
        return None

    def justAddrFields(self,address):
        ret = {}    
        for field in ["address1","address2","city","countryCode","recipient","zip","zoneCode","phone"]:
            if address.get(field,"")=="" and field in ["phone"]:
                continue
            ret[field] = address.get(field)
        return ret    
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
        
        for recordId in self.recordList():
            
            if self.has("onlyTheseIds"):
                if len(self.onlyTheseIds)>0 and recordId not in self.onlyTheseIds:
                    continue       
            customer = self.loadRecord(recordId,prune=True,searchable=True)
            existingRecord = self.loadConsolidateRecord(recordId,searchable=True)
            if existingRecord is None:
                existingRecord = NetSuiteRecord("",blank=True)
                
            
            if "nothing to do" in customer.get("comments",""):
                print(f"customer {customer.id} has nothing to do!")
                continue
            if not customer.contactRoles:
                print(f"customer {customer.id} no contacts!")
                continue
            self.processRecord(customer,existingRecord)
            
    def processRecord(self,customer,existingRecord):
            print(f"processing {customer.recordId}")
            if customer.get("addressBook") is None:
                print("empty address book!")
                if existingRecord:
                    existingRecord.rm()
                return
            try:
                addresses = self.mapAddresses(customer)
                if len(addresses)<1:
                    print("no addresses")
                    return
            except:
                print(customer.get("addressBook"))
                traceback.print_exc()
                return
            billingAddress = self.addressByType(addresses,"billing")
            shippingAddress = self.addressByType(addresses,"shipping")
            if billingAddress and not shippingAddress:
                shippingAddress = billingAddress
            elif shippingAddress and not billingAddress:
                billingAddress = shippingAddress
            elif not billingAddress and not shippingAddress:
                try:
                    billingAddress = addresses[0]
                except:
                    traceback.print_exc()
                notBillings = list(filter(lambda x:x.get("externalId")!=billingAddress.get("externalId"),addresses))
                if len(notBillings)>0:
                    shippingAddress = notBillings[0]
                if not shippingAddress:
                    shippingAddress = billingAddress
            try:
                ignoreLocation = [billingAddress.get("externalId"),shippingAddress.get("externalid")]
            except:
                traceback.print_exc()
                
                sys.exit()
            mainLocation = None
            mainLocation = {
                "externalId":billingAddress.get("externalId"),
                "name":billingAddress.get("address1"),
                "taxExemptions":billingAddress.get("taxExemptions"),
                "shippingAddress":self.justAddrFields(billingAddress),
            }
            if existingRecord.has("companyLocation"):
                existingMainLocation = existingRecord.get("companyLocation")
                if existingMainLocation.get("shopifyid"):
                    mainLocation["shopifyId"] = existingMainLocation.get("shopifyId")                
            
                
            if shippingAddress.get("externalId")==billingAddress.get("externalId"):
                mainLocation["billingSameAsShipping"] = True
            else:
                mainLocation["billingAddress"] = self.justAddrFields(billingAddress)
            
            contacts = self.mapContacts(customer,existingRecord)
            
            mainContact = None
            if len(contacts)>0:
                mainContact = contacts[0]
            else:
                print("No Contacts!")
                if existingRecord is not None:
                    existingRecord.rm()
            ret = {
                "_externalId":str(customer.id),
                "name":customer.companyName,
                "customerSince":customer.dateCreated,
                "contacts":contacts,
                "companyLocation":mainLocation,
                "customFields":customer.get("customFields"),
                "_errors":[],
                "_inactive":customer.get("isInactive",False)
            }
            
            
            existingLocations = {}
            finalLocations = []
            if not existingRecord.blank:
                existingLocations = {location.get("name"):location for location in existingRecord.get("locations")}
            
            for address in addresses:
                
                if address.get("address1") in existingLocations:
                    if existingLocations[address.get("address1")].get("shopifyId"):
                        existingLocations[address.get("address1")]["_externalid"] = address.get("_externalid")
                        finalLocations.append(existingLocations[address.get("address1")])
                    else:
                        if address.get("externalId") not in ignoreLocation:
                            finalLocations.append(
                                {
                                    "note":"imported from netsuite",
                                    "name":f'{address.get("address1")}',
                                    "billingSameAsShipping":True,
                                    "shippingAddress":self.justAddrFields(address),
                                    "_externalId":address.get("_externalId")
                                 }
                            )
            ret["locations"] = finalLocations
            
            if not existingRecord.blank:
                for field in ["shopifyId","shopifyCatalogId"]:
                    if existingRecord.has(field):
                        ret[field] = existingRecord.get(field)
            
            #if customer.get("isPerson"):
                #return
            addresses = list(map(lambda x:x.get("shippingAddress"),ret["locations"]))
            if mainContact is None:
                print("No main contact!")
            
            mainLocation = ret["companyLocation"]
            
            if False:
                    self.writeConsolidatedRecord(
                        customer.recordId,
                        {
                            "addresses":addresses,
                            "firstName":mainContact.get("firstName","No First Name"),
                            "lastName":mainContact.get("lastName"),
                            "email":mainContact.get("email"),
                            "phone":format_phone(mainContact.get("phone")),
                            "tags":["_netsuite-import"],
                            "netSuiteId":customer.id,
                            "customerSince":customer.dateCreated,
                            "orderCount":len(self.orders[customer.id])
                        },
                        forceType="customer"
                    )
            #else:
            print(f"writing {customer.recordId}")
            self.writeConsolidatedRecord(customer.recordId,ret)
        