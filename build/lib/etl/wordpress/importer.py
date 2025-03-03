import csv
import re
import subprocess
import traceback
import requests
import xmltodict
import json
import os
import sys
from jmespath import search as jpath
from bs4 import BeautifulSoup, Comment, NavigableString
from slugify import slugify
import shopify
from shopify_uploader import ShopifyUploader
import paramiko
import mimetypes
import base64
import shopify
from urllib.parse import urlparse,parse_qs
from glob import glob
from PIL import Image,ImageOps
import time
import argparse
import pathlib
import functools

class WordpressImporter:
    def __init__(self,wordpressFile,useCache=True,outputFile=None):
        self.useCache = useCache
        self.input = xmltodict.parse(open(wordpressFile).read().replace("wp:",""))
        self.config_obj = json.load(open("config.json"))
        self.outputFile = outputFile
        self.parsed = {"poasts":[],"pages":[]}
        
        if self.outputFile is not None:
            if pathlib.Path(self.outputFile).exists():
                self.parsed = json.load(open(self.outputFile))
        self.post_handles = [x.get("handle") for x in self.parsed.get("poasts")]
        self.page_handles = [x.get("handle") for x in self.parsed.get("pages")]
    
      
            
    def excludePage(self,handle):
        return False
        
    def excludePost(self,handle):
        return False
        
    
    def config(self,key,default=None):
        return self.config_obj.get(key,default)
    
    def data(self):
        return self.input
    
    def exists(self,post):
        return post.get("handle")
        
    def run(self):
        for post in filter(lambda x:x["post_type"]=="post",jpath("rss.channel.item",self.input)):
            if self.excludePost(post.get("post_name")):
                print(f"Skipping {post.get('post_name')}: excluded")
                continue
            if post.get("post_name") in self.post_handles:
                found = False
                index = 0
                for poast in self.parsed["poasts"]:
                    if poast.get("handle")==post.get("post_name"):
                        if poast.get("shopifyId") is None:
                            found = True
                            details = self.postDetails(post)
                            if details is not None:
                                self.parsed["poasts"][index] = details
                            break
                    index = index+1
                if not found:
                    print(f"Skipping {post.get('post_name')}")            
            else:
                details = self.postDetails(post)
                if details is not None:
                    self.parsed.get("poasts").append(details)
        for page in filter(lambda x:x["post_type"]=="page",jpath("rss.channel.item",self.input)):
            if self.excludePage(page.get("post_name")):
                print(f"Skipping {page.get('post_name')}: excluded")
                continue
            if page.get("content:encoded") is None or page.get("content:encoded")=="":
                continue
            if page.get("post_name") in self.page_handles:
                found = False
                index = 0
                for impage in self.parsed["pages"]:
                    if impage.get("handle")==page.get("post_name"):
                        if impage.get("shopifyId") is None:
                            found = True
                            details = self.postDetails(page)
                            if details is not None:
                                self.parsed["pages"][index] = details
                                break
                    index = index+1
                if not found:
                    print(f"Skipping {page.get('post_name')}")            
            else:
                details = self.postDetails(page)
                if details is not None:
                    self.parsed.get("pages").append(details)
        
        return self
    def cached(self,handle):
        if not self.useCache:
            return None
        if not os.path.isfile(f"/download/{handle}.html"):
            return None
        return open(f"download/{handle}.html").read()
    def cache(self,handle,contents):
        if not self.useCache:
            return
        open(f"download/{handle}.html","w").write("contents")

    def parsed(self):
        return self.parsed
    def write(self,outputFile=None):
        if outputFile is None:
            outputFile = self.outputFile
        
        open(outputFile,"w").write(json.dumps(self.parsed,indent=1))
        return self
    
    def arrayVal(self,array,key):
        array = [array] if type(array) is dict else array
        try:
            ret = list(filter(lambda obj: obj.get(key) is not None,array))
            if len(ret)>0:
                return ret[0]    
        except:
            print(array)
        return None

    def attachment(self,id):
        if id=="" or id is None:
            return None
        
        ret = list(filter(lambda item: item.get("post_type")=="attachment" and item.get("post_id")==id,jpath("rss.channel.item",self.data())))
        if len(ret)>0:
            return ret[0]["attachment_url"]
        return None

    def category(self,id):
        if id=="" or id is None:
            return None
        
        ret = list(filter(lambda item: item.get("term_id")==id,jpath("rss.channel.category",self.data())))
        if len(ret)>0:
            return ret[0]["cat_name"].replace("&amp;","&")
        return None

    def author(self,email):
        if id=="" or id is None:
            return None
        
        ret = list(filter(lambda item: item.get("author_email")==email,jpath("rss.channel.author",self.data())))
        if len(ret)>0:
            return ret[0]["author_display_name"]
        return None
        
    def postMeta(self,post,key):
        meta = [post.get("postmeta")] if type(post.get("postmeta")) is dict else post.get("postmeta")
        ret = list(
            filter(lambda kv:kv.get("meta_key")==key,meta)
        )
        if len(ret)>0:
            return ret[0].get("meta_value")
        return None
    def innerHTML(self,soup):
        innerOuter = soup.find("div",class_="elementor-widget-theme-post-content")
        if innerOuter is not None:
            return innerOuter.find("div",class_="elementor-widget-container")
        return None
        
    def postContent(self,url,handle):
        attempts = 0
        retry = True
        cachedContent = self.cached(handle)
        if cachedContent is not None:
            return cachedContent
        
        while attempts<10 and retry:
            try:
                content = requests.get(
                    url,
                    headers={
                        "Referer":self.config("blog_url"),
                        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                    }
                ).content.decode("utf-8")
                soup = BeautifulSoup(content,'html.parser')
                retry = False
                content = str(self.innerHTML(soup))
                self.cache(handle,content)
                return content
            except:
                traceback.print_exc()
                print(f"retrying {url}",file=sys.stderr)
                attempts = attempts+1
            

    def postDetails(self,post):
        categories = post.get("category",[])
        categories = [categories] if type(categories) is dict else categories
        
        try:
            retval = {
                "title":post.get("title"),
                "handle":post.get("post_name"),
                "status":"active" if post.get("status")=="publish" else "draft",
                "url":post.get("link"),
                "articleImage":self.attachment(self.postMeta(post,"_thumbnail_id")),
                "description":self.postMeta(post,"_yoast_wpseo_metadesc"),
                "category":self.category(self.postMeta(post,"_yoast_wpseo_primary_category")),
                "published":post.get("post_date","").split(" ")[0],
                "excerpt":post.get("excerpt:encoded",""),
                "author":self.author(post.get("dc:creator")),
                "wordpress_id":post.get("post_id"),
                "tags":[tag["#text"] for tag in filter(lambda cat:cat["@domain"]=="post_tag",categories)],
                "categories":[tag["#text"] for tag in filter(lambda cat:cat["@domain"]=="category",categories)],
            }
            if retval["status"]=="active":
                print(f"Downloading from: {post.get('link')}",file=sys.stderr)
                retval["html"]=self.postContent(post.get("link"),retval.get("handle"))
                if retval["html"] is None:
                    return None
                time.sleep(1)
            else:
                retval["html"] = post.get("content:encoded")
            return retval
        except Exception as e:
            traceback.print_exc()
            sys.exit()
            
class NetSuiteImporter:
    pass