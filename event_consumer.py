import requests
import json
from kafka import KafkaConsumer
import sortedcontainers
import logging
import os
import pandas as pd

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
    'binance_wss_topic',
    group_id='binance_wss_group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('ascii'))
    )

# Get depth snapshot:
resp = requests.get('https://api.binance.us/api/v3/depth?symbol=BNBBTC&limit=1000')
depth_snapshot = resp.json()
bid_dict = sortedcontainers.SortedDict(map(lambda t: (float(t[0]), float(t[1])), depth_snapshot['bids']))
ask_dict = sortedcontainers.SortedDict(map(lambda t: (float(t[0]), float(t[1])), depth_snapshot['asks']))
last_update_id = depth_snapshot['lastUpdateId']
print('Depth snapshot last update: ', last_update_id)

def apply_event_to_order_book(event, bid_dict_, ask_dict_):
    if event['b']:
        bids = map(lambda l: (float(l[0]), float(l[1])), event['b'])
        for bid in bids:
            if bid[1] == 0:
                try:
                    bid_dict_.pop(bid[0])
                except KeyError:
                    print('Price level for 0 quantity not present in local order book')
            else:
                bid_dict_.update([bid])
    if event['a']:
        asks = map(lambda l: (float(l[0]), float(l[1])), event['a'])
        for ask in asks:
            if ask[1] == 0:
                try:
                    ask_dict_.pop(ask[0])
                except KeyError:
                    print('Price level for 0 quantity not present in local order book')
            else:
                ask_dict_.update([ask])



for message in consumer:
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    event = message.value
    if event['u'] <= last_update_id:
        print('Event dropped ^')
        continue
    if event['U'] <= last_update_id+1 and event['u'] >= last_update_id+1:
        print('Got first event^') 
        prev_small_u = event['u']
    else:
        assert event['U'] == prev_small_u+1, f'message missing in between {prev_small_u} and {event["U"]}'
        prev_small_u = event['u']
    apply_event_to_order_book(event, bid_dict, ask_dict)
    print(
        'Top 12 bids:\n', pd.DataFrame(
            columns=['price', 'quantity'],
             data=bid_dict.items()[-12:],
              index=range(len(bid_dict.items()[-12:]))
              ).sort_values(by=['price'], ascending=False),
     '\nTop 12 asks:\n', pd.DataFrame(
        columns=['price', 'quantity'],
         data=ask_dict.items()[:12],
          index=range(len(ask_dict.items()[:12]))
          )
    )
