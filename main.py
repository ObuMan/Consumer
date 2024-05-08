from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from azure.eventhub.aio import EventHubConsumerClient
import asyncio
from azure.identity import DefaultAzureCredential
from azure.mgmt.eventhub import EventHubManagementClient
from azure.mgmt.iothub import IotHubClient

app = FastAPI()

credential = DefaultAzureCredential()
subscription_id = '957bbb97-844e-40d6-8155-5dc5b9ddaf69'

client = IotHubClient(credential, subscription_id)

resource_group_name = 'smart-house'
iot_hub_name = 'itestunique123452'

policy_name = 'iothubowner'

keys = client.iot_hub_resource.get_keys_for_key_name(resource_group_name, iot_hub_name, policy_name)

iot_hub = client.iot_hub_resource.get(resource_group_name, iot_hub_name)

properties = iot_hub.properties
endpoints = properties.event_hub_endpoints
endpoint = None

if 'events' in endpoints:
    endpoint = endpoints['events'].endpoint
    print("Endpoint:", endpoint)
else:
    print("Event Hub endpoint not found.")

connection_str = (
    f"Endpoint={endpoint};"
    f"SharedAccessKeyName={policy_name};"
    f"SharedAccessKey={keys.primary_key};"
    f"EntityPath={iot_hub_name}"
)

print("Connection String:", connection_str)

async def handle_event_hub(websocket):
    # connection_str = 'Endpoint=sb://iothub-ns-itestuniqu-58220205-ef5518ff0b.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=LwC9nt7L2fMVMVC1NeoOZSAHSLtcICG7CAIoTPZYIbo=;EntityPath=itestunique123452'
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
            await websocket.send_text(str(event))
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
