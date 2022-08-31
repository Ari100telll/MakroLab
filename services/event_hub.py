import json

from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

from configs.config import settings


class EventHubManager:
    async def send_chunk(self, chunk):
        producer = EventHubProducerClient.from_connection_string(
            conn_str=settings.EVENT_HUB_CONN_STR,
            eventhub_name=settings.EVENT_HUB_NAME
        )
        async with producer:
            event_data_batch = await producer.create_batch()

            for element in chunk:
                event_data_batch.add(EventData(json.dumps(element)))

            await producer.send_batch(event_data_batch)
