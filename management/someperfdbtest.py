# let's start memcached process first
# then let's load n items , mixture of insert and update ?
# then lets call flushctl

import os
from optparse import OptionParser
import time
import random
import uuid
import mc_bin_client
from threading import Thread
from subprocess import Popen, PIPE, STDOUT

#env TIMING_LOG=/tmp/leveldb.fetch.timings ../memcached/memcached -E .libs/ep.so -e 'initfile=t/init.sql;ht_size=1572869' -v


def _load_data_thread(keys,value,op,mc):
   random.shuffle(keys)
   for k in keys:
      if op == 'set':
         mc.set(k,0,0,value)
      elif op == "delete":
         try:
            mc.delete(k)
         except:
            pass

def create_value(pattern, size):
    return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

def load_data(no_items,insert,delete,no_threads,size,ip,port):
   _threads = []
   prefix = str(uuid.uuid4())
   keys=["{0}-{1}".format(prefix,i) for i in range(1000000)]
   value = create_value("*",size)
   mc = mc_bin_client.MemcachedClient(ip,port)
   if insert:
      for i in range(0,no_threads):
         _t = Thread(name="insert-thread-{0}".format(i), target=_load_data_thread, args=(keys,value,'set',mc))
         _threads.append(_t)
   if delete:
      pass
   for t in _threads:
      t.start()
   return _threads

def start_memcached(memcached_process,
                    timing_log,
                    memcached_port,
                    dbtype,
                    dbparam):
   os.environ["TIMING_LOG"] = log
   command = []
   command.append(memcached_process)
   command.append("-p {0}".format(memcached_port))
   command.append("-E")
   command.append(".libs/ep.so")
   command.append("-e")
   engine_params = "{0};{1};".format(dbparam,"ht_size=1572869")
   command.append(engine_params)
   command.append("-v")
   print "command : ",command
   print os.getcwd()
   mc_process = Popen(command)
   return mc_process

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--port", dest="port",default="11211",
                      help="memcached port", metavar="11211")
    parser.add_option("-l", "--log", dest="log",default="/tmp/sqldb.fetch.timings",
                      help="log file to dump timings for fetches", metavar="/tmp/sqldb.fetch.timings")
    parser.add_option("-i", "--items", dest="no_items",default="100000",
                      help="number of items", metavar="100000")
    parser.add_option("-t", "--threads", dest="no_threads",default="1",
                      help="number of threads", metavar="1")
    parser.add_option("-s", "--size", dest="size",default="256",
                      help="value size", metavar="256")


    options, args = parser.parse_args()
    port = int(options.port)
    log = options.log
    no_threads = int(options.no_threads)
    no_items = int(options.no_items)
    size = int(options.size)
    print size,no_items,no_threads,log,port
    start_memcached("../memcached/memcached",log,port,"sqldb","initfile=t/init.sql")
    time.sleep(5)
    threads = load_data(no_items,True,False,no_threads,size,"localhost",port)
    for t in threads:
       t.join()
       print "thread {0} finished".format(t)



