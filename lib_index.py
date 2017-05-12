#!/usr/bin/python
# This Python file uses the following encoding: utf-8

'''
@author: Denis Gudtsov
'''

from riak import *

import string
import random

class Index(object):
    buckets = dict()
    bucket = None
    
    def __init__(self, host=None,port=None,proto='pbc', nodes=None, **kwargs):
        if nodes == None:
            self.myClient = RiakClient(pb_port=port, protocol=proto, host=host)
        else:
            self.myClient = RiakClient(protocol=proto, nodes = nodes)
        return

    def destroy(self):
        self.myClient.close()
        return

 # PRIVATE
    def __bucket(self,bucket,bucket_type='default'):
        if bucket not in self.buckets:
            self.buckets[bucket] = self.myClient.bucket_type(bucket_type).bucket(bucket)
        return self.buckets[bucket]    

    def __get_obj(self,key,bucket=None):
        if bucket==None: 
            obj = RiakObject(self.myClient, self.bucket, key)
        else:
            obj = RiakObject(self.myClient, self.__bucket(bucket), key)
        return obj

 # PUBLIC
 
# creates new bucket with type
    def create_bucket(self,bucket,bucket_type='default'):
        self.__bucket(bucket,bucket_type)
        return self

# assign this bucket as default to object class
    def set_default_bucket(self,bucket,bucket_type='default'):
        self.bucket = self.__bucket(bucket,bucket_type)
        return self.bucket
    
    def create_search_index(self,index_name):
        self.myClient.create_search_index(index_name)
        return self
    
# store new KVP into DB
    def set (self,key,value,bucket=None):
        
        obj = self.__get_obj(key, bucket)
        obj.data = value
        obj.store()
        return self
    
# request stored KVP from DB        
    def get (self, key, bucket=None):
#        obj_bucket = self.myClient.bucket(bucket)
        if bucket==None: 
            return self.bucket.get(key).data
        else:
            return self.__bucket(bucket).get(key).data
    
# clear stored KVP from DB 
    def unset (self,key,bucket=None):
        obj = self.__get_obj(key, bucket)
        obj.delete()
        
        return self    

if __name__ == '__main__':
    
# Example code    

# initialize connection    
    idx = Index('192.168.90.58', 8087 )
        
# OR multihost connection
#    nodes=[{'host':'192.168.90.57','pb_port':8087},
#       {'host':'192.168.90.58','pb_port':8087}
#      ]
#    idx = Index(nodes=nodes_remote)
    
# assign bucket and bucket type
    bucket,bucket_type = 'test4','books'
    
    compex_value = {
        'ckey1':'cvalue1',
        'ckey2':'cvalue2'
    }
    
# creates new bucket with type=animals
# if you omit type name default will be used
 
    idx.create_bucket(bucket,bucket_type)

# store key/value in bucket

    idx.set('key1', 'value1', bucket)
    
# assigning bucket to idx object    
    idx.set_default_bucket(bucket)
# now we can invoke set without bucket name    
    idx.set('key2', 'value2')
    
    idx.set('key3', compex_value)

# requesting key previously stored, bucket name is implicit    
    print idx.get('key1')
    
    print idx.get('key2',bucket)
    
    print idx.get('key3')

# remove KVP
    idx.unset('key1')
    idx.unset('key2')
    idx.unset('key3')
# should return none
    print idx.get('key1')
    
    idx.destroy()
    
    exit