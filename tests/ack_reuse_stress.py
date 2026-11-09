#!/usr/bin/env python3
"""Bounded real-TCP scheduling probe; not a throughput benchmark.

Eight clients reuse each identifier immediately after its final ACK, without a
PING barrier. A failure is an actual protocol trace, not a forced test-only hook.
RMQTT_BIN selects the tested immutable executable through FeatureBroker.
"""
import concurrent.futures
import tempfile
import time
from retained_will_blackbox import FeatureBroker
from mqtt5_blackbox import connect5,publication5,parse_ack5,ack5,readprops
from blackbox import receive
def main():
 with tempfile.TemporaryDirectory(prefix='rmqtt-credit-race-') as directory:
  # Default max_auth_tasks is 4; eight concurrent CONNECT handshakes need headroom
  # or CONNACK 0x89 Server busy flakes the probe before identifier reuse runs.
  broker=FeatureBroker(directory,False,['--max-inflight','1','--max-connections','32',
                                        '--max-handshakes','32','--max-sessions','32',
                                        '--max-auth-tasks','32']);broker.start()
  try:
   def sender(i):
    qos=1 if i<4 else 2;runs=1000 if qos==1 else 500
    with broker.socket() as sock:
     sock.sendall(connect5('reuse-'+str(i)))
     h,b=receive(sock);assert h==0x20 and b[:2]==b'\0\0';properties,_=readprops(b,2);assert (0x21,1) in properties
     for n in range(runs):
      sock.sendall(publication5('stress/'+str(i),n.to_bytes(4,'big'),qos,1))
      parse_ack5(receive(sock),0x40 if qos==1 else 0x50,1)
      if qos==2:
       sock.sendall(ack5(0x62,1));parse_ack5(receive(sock),0x70,1)
    return qos,runs
   start=time.monotonic()
   with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:result=list(pool.map(sender,range(8)))
   print('PASS immediate post-ACK identifier reuse,8 simultaneous clients,4000 Q1+2000 Q2 publications,ReceiveMaximum1,no PING barrier;seconds',round(time.monotonic()-start,3))
  finally:broker.stop()

if __name__ == '__main__':
 main()
