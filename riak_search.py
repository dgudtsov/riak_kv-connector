#!/usr/bin/python
# This Python file uses the following encoding: utf-8

'''
@author: Denis Gudtsov
'''

# here is simple example of usage riak search

from riak import *

from riak.datatypes import Map

import json

Call_proto = {      
      'time_start_i':1494419737,
      'duration_i': 30,
      'A': {'msisdn_s'  : "78881234567",
            'imsi_s'    : "123",
            'imei_s'    : "abc",
            'LU' : [
                {'ts1':'CGI1'},
                {'ts2':'CGI2'},
                {'ts3':'CGI3'}
                ]
        },
      'B': {'msisdn_s'  : "71239995577",
            'imsi_s'    : "456",
            'imei_s'    : "def",
            'LU' : [
                {'ts1':'CGI1'},
                {'ts2':'CGI2'},
                {'ts3':'CGI3'}
                ]
        },
        'callid_s':'1j9FpLxk3uxtm8tn@example.com'
  }

SMS_proto = {
      'time_start_i':1494419737,

      'A': {'msisdn_s'  : "78881234567",
            'imsi_s'    : "123",
            'imei_s'    : "abc",
            'LU' : [
                {'ts1':'CGI1'}
                ]
        },
      'B': {'msisdn_s'  : "71239995577",
            'imsi_s'    : "456",
            'imei_s'    : "def"
        },
        'msgid_s':'1j9FpLxk3uxtm8tn@example.com',
        'msgtext_s':''
}

class Call():

    # call metadata    
    meta = dict()

    def __init__(self,proto):
        # proto - dict
        self.meta = proto.copy()
        self.meta['key'] = self._get_uniq_key()
        return
    
    def _get_uniq_key(self):
        
        # calculating complex key
        ts = self.meta['time_start_i']
        msisdn_a = self.meta['A']['msisdn_s']
        msisdn_b = self.meta['B']['msisdn_s']
        
        return str(msisdn_a)+'_'+str(msisdn_b)+'_'+str(ts)
    
    def update_uniq_key(self):
        self.meta['key'] = self._get_uniq_key()
        return self.meta['key']
    
    def get_json(self):
        return json.JSONEncoder().encode(self.meta)

if __name__ == '__main__':
    
    myClient = RiakClient(pb_port=8087, protocol='pbc', host='192.168.90.57')

    
    # this must be executed before bucket-type creation:
#    myClient.create_search_index('sorm_index')
    
    
# and this must be executed on riak node:
# riak-admin bucket-type update sorm '{"props":{"search_index":"sorm_index","allow_mult":false}}'
# riak-admin bucket-type activate sorm

    bucket = myClient.bucket_type('sorm').bucket('sorm1')
    
    print bucket.get_properties()

    
    call = Call(Call_proto)

    
    print call.meta['key']
    
#    call1.store()

    fetched = bucket.get(call.meta['key'])
#    fetched = bucket.get('liono2')
    
    print fetched.encoded_data

    results = myClient.fulltext_search('sorm_index', 'A.msisdn_s:7888* AND B.msisdn_s:7123* AND time_start_i:[1494419736 TO 1494419737]')
    print results
    print results['docs']

    myClient.close()
    
    exit