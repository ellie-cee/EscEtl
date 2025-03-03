from .base import *

class Customer(GraphQL):
    def create(self,input):
        return self.run(
            """
            mutation createCustomerMetafields($input: CustomerInput!) {
                customerCreate(input: $input) {
                    customer {
                       id
                    }   
                    userErrors {
                        message
                        field
                    }
                }
            }
            """,
            input
        )
    def find(self,email):
        customers = self.run(
            """
            query getCustomers($query:String!) {
                customers(query:$query,first:1) {
                    nodes {
                        id
                        email
                        firstName
                        lastName
                        companyContactProfiles {
                            company {
                                id
                                defaultRole {
                                    id
                                }
                                locations(first:20) {
                                    nodes {
                                        id
                                    }
                                    
                                }
                            }
                            id
                            roleAssignments(first:50) {
                                nodes {
                                    companyLocation {
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
        candidates = customers.search("data.customers.nodes || []")
        if candidates is None:
            return None
        else:
            candidate = next(filter(lambda x:x.get("email")==email,candidates),None)
            if candidate:
                return SearchableDict(candidate)
            return None
    def create(self,input):
        return self.run(
            """
            mutation customerCreate($input: CustomerInput!) {
                customerCreate(input: $input) {
                    userErrors {
                        field
                        message
                    }
                    customer {
                        id
                        email
                        phone
                        taxExempt
                        firstName
                        lastName
                        amountSpent {
                            amount
                            currencyCode
                        }
                        smsMarketingConsent {
                            marketingState
                            marketingOptInLevel
                            consentUpdatedAt
                        }
                    }
                }
            }
            """,
            input
        )
    def update(self,input):
        return self.run(
            """
            mutation updateCustomerMetafields($input: CustomerInput!) {
                customerUpdate(input: $input) {
                    customer {
                        id
                        tags
                    }
                    userErrors {
                        message
                        field
                    }
                }
            }
            """,
            input
        )
    