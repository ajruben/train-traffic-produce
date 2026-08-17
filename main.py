import json
import os
import requests
import time

from dotenv import load_dotenv

from email.utils import formatdate
from confluent_kafka import Producer

load_dotenv()

#NS API
PRIMARY_KEY = os.getenv("PRIMARY_KEY")
VIRTUAL_TRAIN_URL: str =  "https://gateway.apiportal.ns.nl/virtual-train-api/vehicle"
headers= {"Cache-Control":"no-cache",
                          "Ocp-Apim-Subscription-Key":PRIMARY_KEY}

# KAFKA 
BOOTSTRAP: dict = {"bootstrap.servers": os.getenv("BOOTSTRAP")}
TOPIC: str = "traffic-event"

def train_message(err, msg):
    if err is not None:
        print(f"Train message derailed {str(err), str(msg)}")
    else:
        print(f"Tain message published {str(msg)}")

def main() -> None:
    p: Producer = Producer(BOOTSTRAP)
    try:
        while True:
            try:
                # catch train 
                response = requests.request(
                    method="GET",
                    url = VIRTUAL_TRAIN_URL,
                    headers = headers
                )
                response.raise_for_status()

                # caught the train :D
                payload = response.content
                response_headers = response.headers
                date_response = response_headers.get("Date")
                train_arrived = response.ok
            
            # missed the train :( 
            except requests.RequestException as e:
                print(f"fetch failed: {e}")
                payload = json.dumps({"payload":{"treinen":[]}}).encode()
                date_response= formatdate(usegmt=True)
                train_arrived = False

            #kafka needs bytes
            date_response= str(date_response).encode()
            train_arrived = str(train_arrived).encode()

            #all aboard and choo choo to kafka
            p.produce(topic=TOPIC, value=payload, headers={'event_time':date_response, 'response_ok':train_arrived}, callback=train_message) #immediate
            
            #track state of choo choo msg
            p.poll(timeout=2)

            time.sleep(8)    
    except KeyboardInterrupt:
        print("Someone pulled the emergency break, train coming to a halt. (KeyboardInterrupt)")
    finally:
        p.flush() # make sure every queed message is delivered


if __name__ == "__main__":
    main()
