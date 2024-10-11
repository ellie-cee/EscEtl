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
        candidates = customers.search("data.customers.nodes || []")
        if candidates is None:
            return None
        else:
            candidate = next(filter(lambda x:x.get("email")==email,candidates),None)
            if candidate:
                return SearchableDict(candidate)
            return None
    