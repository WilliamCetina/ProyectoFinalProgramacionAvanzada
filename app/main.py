from fastapi import FastAPI
import pika
import json
import os

app = FastAPI()

RABBIT_URL = os.getenv("RABBIT_URL")

@app.get("/procesar/{video_name}")
def procesar_video(video_name: str):
    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    message = {"video": video_name}
    channel.basic_publish(
        exchange="",
        routing_key="video_queue",
        body=json.dumps(message)
    )

    connection.close()
    return {"status": "tarea enviada", "video": video_name}
