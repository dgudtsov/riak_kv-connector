#!/usr/bin/python
# This Python file uses the following encoding: utf-8

'''
@author: Denis Gudtsov
'''

# this is simple benchmark test fir riak.

from lib_index import *
from riak_search import *

from multiprocessing import Process, freeze_support
import os

from timeit import default_timer as timer

import string
import random

#sys.path.append('/home/denis/soft/liclipse/pyvmmonitor/public_api')
#import pyvmmonitor
#pyvmmonitor.connect()

bucket = 'sorm3'
bucket_type = 'sorm'

value_size = 1024
kvp_pairs = 1000

#
proc_num = 10

#key_format = 'key_{pid}_{id}'
#value_format = '"value":"{}"'

def get_int(r=11):
    return ''.join(random.choice(string.digits) for _ in range(r))

def run_test (idx):

    pid = os.getpid()
#    print "pid: " , pid
#    pids.append(pid)

# todo: optimize random

    value = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(value_size))    
#    print json_value
 
    
    for i in range(kvp_pairs):

#        json_value = '{'+ value_format.format(value) +'}'

        call = Call(Call_proto)
        
        call.meta['time_start_i'] = pid+i
        
        call.meta['A']['msisdn_s'] = get_int(11)
        call.meta['A']['imsi_s'] = get_int(15)
        call.meta['A']['imei_s'] = get_int(15)

        call.meta['B']['msisdn_s'] = get_int(11)
        call.meta['B']['imsi_s'] = get_int(15)
        call.meta['B']['imei_s'] = get_int(15)
        
        call.meta['callid_s'] = value
        
        call.update_uniq_key()
                
#        json_value = json.JSONEncoder().encode(call.meta)
        
#        idx.set(key_format.format(pid=pid,id=i), call.get_json(), bucket)
        idx.set(call.update_uniq_key(), call.meta, bucket)
    return

if __name__ == '__main__':
    freeze_support()
    
#    print get_int(11)

    nodes_remote=[{'host':'192.168.90.57','pb_port':8087},
       {'host':'192.168.90.58','pb_port':8087}
      ]

    nodes_local=[{'host':'10.10.10.101','pb_port':8087},
       {'host':'10.10.10.102','pb_port':8087},
       {'host':'10.10.10.103','pb_port':8087},
       {'host':'10.10.10.104','pb_port':8087},
       {'host':'10.10.10.105','pb_port':8087},
       {'host':'10.10.10.106','pb_port':8087}
      ]


#    idx = Index( '192.168.90.58', 8087 )
#    idx = Index(nodes=nodes_local)
    idx = Index(nodes=nodes_remote)
    
#    idx.create_bucket(bucket)
#    idx.create_search_index('sorm_index')
    idx.create_bucket(bucket,bucket_type)
                
#    run_test(idx)
                
    processes = [
        Process(target=run_test, args=(idx,))
        for i in range(proc_num)
    ]

    print "starting all processes, count=", len(processes)
        
    start = timer()
    map(lambda p: p.start(), processes)
    
    print "started at ", start
    print "all processes are running now, waiting..."
    
    map(lambda p: p.join(), processes)
    end = timer()
    
    print "finished at ", end
    print "total, seconds: ",(end - start)
    print "total kvp wrote: ", kvp_pairs*(proc_num)
    print "each kvp size: ", value_size
    print "TPS: ",kvp_pairs*(proc_num)/(end - start)
            
    idx.destroy()
    exit
