from .base import *

class Companies(GraphQL):
    
    def paymentTermsTemplates(self):
        return self.run(
            """
            query {
                paymentTermsTemplates {
                    id
                    name
                    paymentTermsType
                    dueInDays
                    description
                    translatedName
                }
            }
            """
        )
    def setLocationTerms(self,input):
        
        return self.run(
            """
            mutation companyLocationUpdate($companyLocationId: ID!, $input: CompanyLocationUpdateInput!) {
                companyLocationUpdate(companyLocationId: $companyLocationId, input: $input) {
                    companyLocation {
                        id
                        buyerExperienceConfiguration {
                            paymentTermsTemplateId    
                        }
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
    def catalogs(self,type="COMPANY_LOCATION"):
        return self.run(
            """
            query getCatalogs($type:CatalogType) {
                catalogs(first:100,type:$type) {
                    nodes {
                        id
                        title
                        priceList {
                            id
                            parent {
                                adjustment {
                                    type
                                    value
                                }
                            }
                        }
                    }
                }
            }
            """,
            {"type":type}
        )
    def updateCatalog(self,input):
        return self.run(
            """
            mutation catalogUpdate($id: ID!, $input: CatalogUpdateInput!) {
                catalogUpdate(id: $id, input: $input) {
                    catalog {
                        id
                        title
                        priceList {
                            id
                            parent {
                                adjustment {
                                    type
                                    value
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
            input
        )
        
    def addLocationsToCatalog(self,catalogId,locationIds):
        return self.run(
            """
            mutation catalogContextUpdate($catalogId: ID!,$contextsToAdd:CatalogContextInput) {
                catalogContextUpdate(catalogId: $catalogId,contextsToAdd:$contextsToAdd) {
                    catalog {
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
                "catalogId": catalogId,
                "contextsToAdd": {
                    "companyLocationIds": locationIds,
                },
            }
        )
    def getAll(self):
        return self.iterable(
            """
            query getCompanies($after:String) {
                companies(first:50,after:$after) {
                    nodes {
                        id
                        name
                        netsuiteId: metafield(namespace:"cnr",key:"netsuite_id") {
                            value
                        }
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }     
                }
            }
            """,
            {"after":None}
        )
    def get(self,companyId):
        return self.run(
            """
            query getCompany($id:ID!) {
                company(id:$id) {
                    id
                    name
                    externalId
                    mainContact {
                        id
                        customer {
                            id
                            email
                            firstName
                            lastName
                        }
                    }
                    defaultRole {
                        id
                        name
                        note
                    }
                    contacts(first:100) {
                        nodes {
                            id
                            customer {
                                id
                                email
                            }
                            roleAssignments(first:50) {
                                nodes {
                                    companyLocation {
                                        id
                                    }
                                    id
                                }
                            }
                        }
                    }
                    contactRoles(first:10) {
                        nodes {
                            id
                            name
                            note
                            
                        }
                    }
                    draftOrders(first:100) {
                        nodes {
                            id
                        }
                    }
                    locations(first: 10) {
                        nodes {
                            id
                            name
                            shippingAddress {
                                firstName
                                lastName
                                address1
                                city
                                province
                                zip
                                country
                            }
                            billingAddress {
                                firstName
                                lastName
                                address1
                                city
                                province
                                zip
                                country
                            }
                        }
                    }
                } 
            }
            """,
            {"id":companyId}
        )
    def createCompany(self,input):
    
    
        return self.run(
                """
                mutation CompanyCreate($input: CompanyCreateInput!) {
                    companyCreate(input: $input) {
                        company {
                            id
                            name
                            externalId
                            mainContact {
                                id
                                customer {
                                    id
                                    email
                                    firstName
                                    lastName
                                }
                            }
                            defaultRole {
                                id
                                name
                                note
                            }
                            locations(first: 1) {
                                nodes {
                                    id
                                    name
                                    shippingAddress {
                                        firstName
                                        lastName
                                        address1
                                        city
                                        province
                                        zip
                                        country
                                    }
                                }
                            }
                        }
                        userErrors {
                            field
                            message
                            code
                        }
                    }
                }
                """,
                input)
    def addLocation(self,input):
        return self.run(
                """
                mutation companyLocationCreate($companyId: ID!, $input: CompanyLocationInput!) {
                    companyLocationCreate(companyId: $companyId, input: $input) {
                        companyLocation {
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
        
    def addContact(self,input):
        return self.run(
                """
                mutation companyContactCreate($companyId: ID!, $input: CompanyContactInput!) {
                companyContactCreate(companyId: $companyId, input: $input) {
                        companyContact {
                            id
                            customer {
                                id
                            }
                        }
                        userErrors {
                            code
                            field
                            message
                        }
                    }
                }
                """,
                input
            )
    def assignMainContact(self,input):
        return self.run(
                """
                mutation companyAssignMainContact($companyContactId: ID!, $companyId: ID!) {
                    companyAssignMainContact(companyContactId: $companyContactId, companyId: $companyId) {
                        company {
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
    def findAndAssignContact(self,company,email):
        customers = self.run(
            """
            query getCustomers($query:String!) {
                customers(query:$query,first:1) {
                    nodes {
                        id
                        email
                        companyContactProfiles {
                            company {
                                contacts(first:1,query:$query) {
                                    nodes {
                                        id
                                    }
                                }
                            }
                        }
                    }
                }
            }
            """,
            {"query":f"email:{email}"},
            searchable=True
        )
        
        customerId = customers.search("data.customers.nodes[0].id")  
        companyContactId = customers.search("data.customers.nodes[0].companyContactProfiles[0].company.contacts.nodes[0].id")
        if companyContactId is None:
            
            ret = self.run(
                    """
                    mutation companyAssignCustomerAsContact($companyId: ID!, $customerId: ID!) {
                        companyAssignCustomerAsContact(companyId: $companyId, customerId: $customerId) {
                            companyContact {
                                id
                                customer {
                                    id
                                }
                            }
                            userErrors {
                                code
                                field
                                message
                            }
                        }
                    }
                    """,
                    {
                        "companyId":company,
                        "customerId":customerId
                    },
                    searchable=True
                )
            companyContactId = ret.search("data.companyAssignCustomerAsContact.companyContact.id")
            if companyContactId is None:
                return ret
                
            
        if companyContactId is not None:
            return {
                "customerId":customerId,
                "companyContactId":companyContactId
            }
        
        return None
    def assignContactToLocation(self,location,contact,role):
        return self.run(
                """
                mutation companyLocationAssignRoles($companyLocationId: ID!, $rolesToAssign: [CompanyLocationRoleAssign!]!) {
                    companyLocationAssignRoles(companyLocationId: $companyLocationId, rolesToAssign: $rolesToAssign) {
                        roleAssignments {
                        id
                        }
                        userErrors {
                            code
                            field
                            message
                        }
                    }
                }
                """,
                {
                    "companyLocationId":location,
                    "rolesToAssign":[
                        {
                            "companyContactId":contact,
                            "companyContactRoleId":role
                        }
                    ]
                }
            )
    def deleteCompany(self,companyId):
        return self.run(
            """
            mutation companyDelete($id: ID!) {
                companyDelete(id: $id) {
                    deletedCompanyId
                    userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {"id":companyId}
        )
    def deleteLocations(self,locationIds):
        return self.run(
            """
            mutation companyLocationsDelete($companyLocationIds: [ID!]!) {
                companyLocationsDelete(companyLocationIds: $companyLocationIds) {
                    deletedCompanyLocationIds
                        userErrors {
                        field
                        message
                    }
                }
            }
            """,
            {
                "companyLocationIds":locationIds
            }
        )
    def getByExternalId(self,externalId):
        ret = self.run(
            """
            query getCompanyByExternalId($query:String!) {
                companies(first:1,query:$query) {
                    nodes {
                        id
                        defaultRole {
                            id
                        }
                        locations(first:3) {
                            nodes {
                                id
                            }
                        }
                        contacts(first:20) {
                            nodes {
                                customer {
                                    email
                                }
                                id
                            }
                        }
                        
                    }
                }
            }
            """,
            {"query":f"external_id:{externalId}"},
            searchable=True
        )
        return GqlReturn(ret.search("data.companies.nodes[0]",{}))
    
    def setMainContact(self,input):
        return self.run(
            """
            mutation companyAssignMainContact($companyContactId: ID!, $companyId: ID!) {
                companyAssignMainContact(companyContactId: $companyContactId, companyId: $companyId) {
                    company {
                        mainContact {
                            id
                            customer {
                                email
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
            input
        )
        
        