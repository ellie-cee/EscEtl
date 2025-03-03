from .base import *

class Navigation(GraphQL):
    def create(self,input):
        return self.run(
            """
            mutation CreateMenu($title: String!, $handle: String!, $items: [MenuItemCreateInput!]!) {
                menuCreate(title: $title, handle: $handle, items: $items) {
                    menu {
                        id
                        handle
                        items {
                            id
                            title
                            items {
                                id
                                title
                            }
                        }
                    }
                    userErrors {
                        message
                        code
                        field
                    }
                }
            }
            """,
            input
        )