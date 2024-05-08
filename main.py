from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from azure.eventhub.aio import EventHubConsumerClient
import asyncio

app = FastAPI()

async def handle_event_hub(websocket):
    connection_str = 'Endpoint=sb://iothub-ns-itestuniqu-58220205-ef5518ff0b.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=LwC9nt7L2fMVMVC1NeoOZSAHSLtcICG7CAIoTPZYIbo=;EntityPath=itestunique123452'
    consumer_group = "$Default"
    eventhub_name = "itestunique123452"

    client = EventHubConsumerClient.from_connection_string(
        conn_str=connection_str,
        consumer_group=consumer_group,
        eventhub_name=eventhub_name
    )

    async def on_event(partition_context, event):
        if event:
            event_data = event.body_as_str()
            print(f"Received event: {event_data}")
            await websocket.send_text(f"Received event: {event_data}")
        else:
            print("No event received.")

    async with client:
        await client.receive(on_event=on_event, starting_position="-1")

@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        await handle_event_hub(websocket)
    except WebSocketDisconnect:
        print("WebSocket disconnected")
    finally:
        await websocket.close()
