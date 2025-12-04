from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
import pika
import json
import os
import boto3
import zipfile
import tempfile

app = FastAPI()

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
AWS_REGION = os.getenv("AWS_REGION")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET")

print("DEBUG AWS_REGION =", AWS_REGION)
print("DEBUG S3_OUTPUT_BUCKET =", S3_OUTPUT_BUCKET)
if not AWS_REGION or not S3_OUTPUT_BUCKET:
    raise RuntimeError("Faltan las variables AWS_REGION o S3_OUTPUT_BUCKET")

s3 = boto3.client("s3", region_name=AWS_REGION)

@app.get("/procesar/{video_name}")
def procesar_video(video_name: str):
    params = pika.URLParameters(RABBIT_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue="videos", durable=True)

    message = {"video": video_name}
    channel.basic_publish(
        exchange="",
        routing_key="videos",
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )

    connection.close()
    return {"status": "tarea enviada", "video": video_name}

@app.get("/frames_zip/{video_name}")
def get_frames_zip(video_name: str):
    print("DEBUG AWS_REGION =", os.getenv("AWS_REGION"))
    print("DEBUG S3_OUTPUT_BUCKET =", os.getenv("S3_OUTPUT_BUCKET"))

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))
    bucket = os.getenv("S3_OUTPUT_BUCKET")
    
    prefix = f"{video_name}_"  # ← ARREGLADO
    print("DEBUG prefix =", prefix)

    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    print("DEBUG objects =", objects)

    if "Contents" not in objects:
        raise HTTPException(status_code=404, detail="No se encontraron imágenes para ese video")

    zip_path = f"/app/{video_name}.zip"
    print("DEBUG zip_path =", zip_path)

    with zipfile.ZipFile(zip_path, "w") as zipf:
        for obj in objects["Contents"]:
            key = obj["Key"]
            print("DEBUG descargando =", key)

            _, tmpname = tempfile.mkstemp()
            s3.download_file(bucket, key, tmpname)
            zipf.write(tmpname, arcname=os.path.basename(key))

    print("DEBUG ZIP finalizado")

    return FileResponse(
        zip_path,
        media_type="application/zip",
        filename=f"{video_name}.zip"
    )
