import json
import kafka
from kafka.errors import KafkaError
import time
import requests

from websocket import create_connection
ws = create_connection("wss://stream.binance.us:9443/ws/bnbbtc@depth")

# mock_events = [
#     {'e': 'depthUpdate', 'E': 1665872563205, 's': 'BNBBTC', 'U': 254177919, 'u': 254177919, 'b': [], 'a': [['0.01412980', '25.44000000']]},
#     {'e': 'depthUpdate', 'E': 1665872566205, 's': 'BNBBTC', 'U': 254177920, 'u': 254177920, 'b': [], 'a': [['0.01409500', '5.75000000']]},
#     {'e': 'depthUpdate', 'E': 1665872577206, 's': 'BNBBTC', 'U': 254177921, 'u': 254177921, 'b': [], 'a': [['0.01409540', '0.98000000']]},
#     {'e': 'depthUpdate', 'E': 1665872579206, 's': 'BNBBTC', 'U': 254177922, 'u': 254177923, 'b': [], 'a': [['0.01409500', '6.73000000'], ['0.01409540', '0.00000000']]},
#     {'e': 'depthUpdate', 'E': 1665872580206, 's': 'BNBBTC', 'U': 254177924, 'u': 254177925, 'b': [], 'a': [['0.01409490', '6.00000000'], ['0.01409500', '0.98000000']]},
#     {'e': 'depthUpdate', 'E': 1665872611208, 's': 'BNBBTC', 'U': 254177940, 'u': 254177940, 'b': [], 'a': [['0.01409480', '5.97000000']]},
#     {'e': 'depthUpdate', 'E': 1665872612208, 's': 'BNBBTC', 'U': 254177941, 'u': 254177941, 'b': [], 'a': [['0.01412390', '32.64000000']]},
#     {'e': 'depthUpdate', 'E': 1665872641210, 's': 'BNBBTC', 'U': 254177948, 'u': 254177948, 'b': [], 'a': [['0.01409480', '5.92000000']]},
# ]

producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: json.dumps(m).encode('ascii'))

i = 0
while True:
    #result = mock_events[i % len(mock_events)]
    try:
        result = ws.recv()
        result = json.loads(result)
        print ("Received '%s'" % result)
        future = producer.send('binance_wss_topic', result)
        record_metadata = future.get(timeout=10)
        print(record_metadata)
        time.sleep(1)
    except KeyboardInterrupt:
        break
    i+=1

ws.close()
print('\nwebsocket session closed')