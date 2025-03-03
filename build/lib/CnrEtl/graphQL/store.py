from .base import *

class ShopifyStore(GraphQL):
    def locations(self):
        ret = self.run(
            """
            query getLocations {
                locations(first:50) {
                    nodes {
                        id
                        name
                    }
                }
            }
            """
        )
        return {x.get("name"):x.get("id") for x in ret.search("data.locations.nodes",[])}
    