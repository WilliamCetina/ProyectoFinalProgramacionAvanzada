import pika
import boto3
import cv2
import numpy as np
import os
import json

RABBIT_URL = os.getenv("RABBIT_URL")
AWS_REGION = os.getenv("AWS_REGION")
S3_INPUT_BUCKET = os.getenv("S3_INPUT_BUCKET")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET")
WATERMARK_TEXT = os.getenv("WATERMARK_TEXT")

s3 = boto3.client("s3", region_name=AWS_REGION)

def agregar_marca(frame):
    font = cv2.FONT_HERSHEY_SIMPLEX
    cv2.putText(frame, WATERMARK_TEXT, (10,30), font, 1, (255,255,255), 2)
    return frame

def procesar_video(video_name):
    # 1. Descargar video
    local_video = f"/tmp/{video_name}"
    s3.download_file(S3_INPUT_BUCKET, video_name, local_video)

    cap = cv2.VideoCapture(local_video)
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out_path = f"/tmp/processed-{video_name}"
    out = cv2.VideoWriter(out_path, fourcc, 20.0, (640,480))

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame = cv2.resize(frame, (640,480))
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)
        final = agregar_marca(gray)

        out.write(final)

    cap.release()
    out.release()

    # 3. Subir a S3
    s3.upload_file(out_path, S3_OUTPUT_BUCKET, f"processed-{video_name}")
    print("Video procesado y subido")

def callback(ch, method, properties, body):
    data = json.loads(body)
    procesar_video(data["video"])
    ch.basic_ack(method.delivery_tag)

def main():
    import time
    import pika.exceptions

    print("[WORKER] Iniciando...")

    # Reintentos automáticos
    while True:
        try:
            print("[WORKER] Intentando conectar a RabbitMQ...")
            params = pika.URLParameters(RABBIT_URL)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            break
        except pika.exceptions.AMQPConnectionError as e:
            print("[WORKER] RabbitMQ no está listo. Reintentando en 5 segundos...")
            time.sleep(5)

    channel.queue_declare(queue="video_queue")
    print("[WORKER] Conectado a RabbitMQ. Esperando tareas...")

    channel.basic_consume(queue="video_queue", on_message_callback=callback)
    channel.start_consuming()

if __name__ == "__main__":
    main()
