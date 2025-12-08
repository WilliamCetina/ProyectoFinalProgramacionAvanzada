from fastapi import FastAPI, HTTPException, BackgroundTasks, Path
from fastapi.responses import FileResponse
import pika
import json
import os
import boto3
import zipfile
import tempfile
import re

app = FastAPI()

def validate_name(name: str):
    if not re.fullmatch(r"[A-Za-z0-9_\-\.]{1,100}", name):
        raise HTTPException(status_code=400, detail="Nombre inválido")
    if ".." in name or "/" in name or "\\" in name:
        raise HTTPException(status_code=400, detail="Nombre inválido")
    return name

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
AWS_REGION = os.getenv("AWS_REGION")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET")

if not AWS_REGION or not S3_OUTPUT_BUCKET:
    raise RuntimeError("Faltan las variables AWS_REGION o S3_OUTPUT_BUCKET")

s3 = boto3.client("s3", region_name=AWS_REGION)

@app.get("/procesar/{video_name}")
def procesar_video(
    video_name: str = Path(..., description="Nombre del video a procesar")
):
    video_name = validate_name(video_name)
    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="videos", durable=True)
    message = {"video": video_name}
    channel.basic_publish(
        exchange="",
        routing_key="videos",
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # persistente
    )

    connection.close()
    return {"status": "tarea enviada", "video": video_name}

@app.get("/frames_zip/{video_name}")
def get_frames_zip(
    video_name: str = Path(..., description="Nombre del video")
):
    video_name = validate_name(video_name)

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    bucket = os.getenv("S3_OUTPUT_BUCKET")

    prefix = f"{video_name}_"
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in objects:
        raise HTTPException(status_code=404, detail="No se encontraron imágenes para ese video")

    zip_path = f"/app/{video_name}.zip"

    with zipfile.ZipFile(zip_path, "w") as zipf:
        for obj in objects["Contents"]:
            key = obj["Key"]
            _, tmpname = tempfile.mkstemp()
            s3.download_file(bucket, key, tmpname)
            zipf.write(tmpname, arcname=os.path.basename(key))

    print("DEBUG ZIP finalizado")

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"{video_name}.zip"
    )
