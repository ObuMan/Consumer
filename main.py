from fastapi import FastAPI, WebSocket
from typing import List
from azure.eventhub import EventHubConsumerClient

app = FastAPI()


# Define your event processing callback
async def on_event(partition_context, event):
    return event.body_as_str()


@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connection_str = 'Endpoint=sb://iothub-ns-itestuniqu-58220205-ef5518ff0b.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=LwC9nt7L2fMVMVC1NeoOZSAHSLtcICG7CAIoTPZYIbo=;EntityPath=itestunique123452'
    consumer_group = "$Default"  # The consumer group you want to read from
    eventhub_name = "itestunique123452"
    # Create an instance of EventHubConsumerClient
    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_str,
        consumer_group=consumer_group,
        eventhub_name=eventhub_name
    )

    try:
        # Start the consumer
        async with client:
            # Receive events from all partitions asynchronously
            async for event in client.receive(on_event=on_event):
                print(event)
                await on_event(None, event, websocket)
    except Exception :
        client.close()
        print(Exception + "AAAA")
        return

    client.close()
