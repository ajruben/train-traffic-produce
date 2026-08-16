import asyncio
import json
import os

from confluent_kafka.aio import AIOProducer
from dotenv import load_dotenv

load_dotenv()
PRIMARY_KEY = os.getenv("PRIMARY_KEY")
BOOTSTRAP: dict = {"bootstrap.servers": os.getenv("BOOTSTRAP")}
TOPIC: str = "traffic-event"


async def main() -> None:
    p: AIOProducer = AIOProducer(BOOTSTRAP)
    try:
        # test payload
        payload = json.dumps({"source": "test_api", "status": "test"}).encode()

        delivery_future = await p.produce(topic=TOPIC, value=payload)
        delivered_future = await delivery_future
        await p.flush()
        print(f"sent payload {payload}")
    finally:
        await p.close()


if __name__ == "__main__":
    asyncio.run(main())
