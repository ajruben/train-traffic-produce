import json
import os
import requests
import time

from dotenv import load_dotenv

from email.utils import formatdate
from confluent_kafka import Producer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

from schemas.train import Train, Trains

#NS API
PRIMARY_KEY = os.getenv("PRIMARY_KEY", "").strip()
VIRTUAL_TRAIN_URL: str =  "https://gateway.apiportal.ns.nl/virtual-train-api/vehicle"
headers= {"Cache-Control":"no-cache",
                          "Ocp-Apim-Subscription-Key":PRIMARY_KEY}
TIMEOUT: int = 10

# try get back on track
_RETRY_STRATEGY = Retry(
    total=5,
    connect=5,
    read=5,
    status=5,
    other=0,
    redirect=0,
    backoff_factor=2,
    status_forcelist=[429] + list(range(500, 600)),
    allowed_methods=["GET"],
    raise_on_status=False,
)

# KAFKA
BOOTSTRAP: dict = {"bootstrap.servers": os.getenv("BOOTSTRAP")}
TOPIC: str = "traffic-event"

def train_message(err, msg):
    if err is not None:
        print(f"Train message derailed {str(err), str(msg)}")
    else:
        print(f"Tain message published {str(msg.topic())} | {msg.partition()} | {msg.offset()}")

def main() -> None:
    p: Producer = Producer(BOOTSTRAP)
    session = requests.Session()
    session.headers.update(headers)
    adapter = HTTPAdapter(max_retries=_RETRY_STRATEGY)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        while True:
            try:
                # catch trains
                response = session.get(VIRTUAL_TRAIN_URL, timeout=TIMEOUT)
                response.raise_for_status()

                # caught the trains :D
                payload = response.content
                date_response = response.headers.get("Date")
                event_time = date_response.encode() if date_response else None

                # inspect the train
                trains = Trains.model_validate_json(payload).payload.treinen
                chad_trains = 0
                tRaInS = 0

                if trains:
                    for train in trains:
                        try:
                            Train.model_validate(train)
                            chad_trains += 1
                        except Exception as e:
                            print(f"Train does not look like a train. At least pydantic says so: {e}")
                            tRaInS += 1
                            continue

                        #kafka needs bytes
                        train_nr = str(train.get("treinNummer")).encode()
                        train_msg = json.dumps(train).encode()

                        #all aboard and choo choo to kafka
                        p.produce(topic=TOPIC, key=train_nr, value=train_msg, headers={'event_time':event_time}, callback=train_message) #immediate

                        #track state of choo choo msg
                        p.poll(0)
                    print(f"batch: {chad_trains} produced, {tRaInS} skipped")
                else:
                    print("where them trains at? NS API worked but no trains??")
                    time.sleep(10)
                    continue
                time.sleep(10)

            # missed the trains :(
            except requests.RequestException as e:
                print(f"fetch failed: {e}")
                time.sleep(5)

    except KeyboardInterrupt:
        print("Someone pulled the emergency break, train coming to a halt. (KeyboardInterrupt)")
    finally:
        p.flush() # make sure every queed message is delivered


if __name__ == "__main__":
    main()
