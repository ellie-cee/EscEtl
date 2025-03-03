import json
import subprocess
from ..netsuiteClient import *
from ..consolidators import *
from etl.graphQL import Customer,Companies,MetaField
from etl.misc import *
from shopify_uploader import ShopifyUploader
import urllib.parse

class CollectionsCreator(RecordAwareClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.shopifyInit()
    