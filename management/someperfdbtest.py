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
#./management/mbflushctl localhost:11211 set mem_low_wat 5000000
#./management/mbflushctl localhost:11211 set mem_high_wat 1300000000

def run_mbflushctl(ip,port,ctl):
   command = []
   command.append("./management/mbflushctl")
   command.append("{0}:{1}".format(ip,port))
   command.extend(ctl) 
   print "command : ",command
   print os.getcwd()
   mbctl = Popen(command)
   return mbctl

def wait_till_no_lines(file,no_lines,timeout=600):
   start = time.time()
   while (time.time() - start) < timeout:
      lines_count = sum(1 for line in open(file))
      print "# of lines in file {0} : {1}".format(lines_count,file)
      if lines_count > no_lines:
         return True
      else:
         time.sleep(5)
   return False

def _load_data_thread(keys,value,op,command_center,mc):
   random.shuffle(keys)
   while command_center["state"] == "run":
      for k in keys:
         if op == 'set':
            mc.set(k,0,0,value)
         elif op == "delete":
            try:
               mc.delete(k)
            except:
               pass
         elif op == "get":
            try:
               mc.get(k)
            except:
               pass

def create_value(pattern, size):
    return (pattern * (size / len(pattern))) + pattern[0:(size % len(pattern))]

def load_data(no_items,insert,delete,no_threads,size,ip,port):
   _threads = []
   prefix = str(uuid.uuid4())
   keys=["{0}-{1}".format(prefix,i) for i in range(no_items)]
   value = create_value("*",size)
   command_center = {"state":"run"}
   if insert:
      for i in range(0,no_threads):
         mc = mc_bin_client.MemcachedClient(ip,port)
         _t = Thread(name="insert-thread-{0}".format(i), target=_load_data_thread, args=(keys,value,'set',command_center,mc))
         _threads.append(_t)
   if delete:
      pass
   for t in _threads:
      t.start()
      print "started thread {0}".format(t)
   return _threads,keys,command_center


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
   #kill memcached processif one is running
   kill_mc = Popen(["killall","-9","memcached"])
   kill_mc.communicate()
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
    parser.add_option("--low_watermark", dest="low",default="1000000",
                      help="memory low watermark", metavar="1000000")
    parser.add_option("--high_watermark-", dest="high",default="3000000",
                      help="memory high watermark", metavar="3000000")


    options, args = parser.parse_args()
    port = int(options.port)
    log = options.log
    no_threads = int(options.no_threads)
    no_items = int(options.no_items)
    size = int(options.size)
    low = int(options.low)
    high = int(options.high)
    print size,no_items,no_threads,log,port
    mc_process = start_memcached("../memcached/memcached",log,port,"sqldb","initfile=t/init.sql")
    time.sleep(5)
    threads,keys,command_center = load_data(no_items,True,False,no_threads,size,"localhost",port)
    command_center["state"] = "stop"
    abort_on_no_lines = (no_items * no_threads) * 0.9 
    print "abort_on_no_lines : {0}".format(abort_on_no_lines)
    wait_till_no_lines(log,int(abort_on_no_lines),timeout=600)
    for t in threads:
       t.join()
       print "thread {0} finished".format(t)
    ctl1 = run_mbflushctl("localhost",port,["set","mem_low_wat","{0}".format(low)])
    ctl2 = run_mbflushctl("localhost",port,["set","mem_high_wat","{0}".format(high)])
    ctl1.communicate()
    print "ctl1 completed"
    ctl2.communicate()
    print "ctl2 completed"
    #now pass all those keys to a new thread which is going to read the data
    mc = mc_bin_client.MemcachedClient("localhost",port)
    fetch_thread = Thread(name="fetch-thread",target=_load_data_thread,args=(keys,"","get",mc))
    fetch_thread.start()
    fetch_thread.join()
    print "thread {0} finished".format(fetch_thread)
    mc_process.terminate()
