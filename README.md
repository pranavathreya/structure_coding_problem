# How To Run The App
### Step 1: Create and Activate Python Virtual Environment
```virtualenv --python=python3.8 .venv; source .venv/bin/activate```
### Step 2: Install Necessary Packages
```python -m pip install -r requirements.txt```
### Step 3: Boot Kafka (needs docker-compose):
```docker-compose -f kafka.yml up -d```
### Step 4: Kick off the Binance Websocket Stream to Kafka
```python event_sender.py```
### Step 5: In Another Terminal, Kick off the Top 12 Bid/Ask Limit Order Display
```python event_consumer.py```