import pika
import boto3
import cv2
import numpy as np
from multiprocessing import Pool, cpu_count, set_start_method
import os
import json
import base64
import time

# usar spawn para Docker
try:
    set_start_method("spawn")
except RuntimeError:
    pass

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_INPUT_BUCKET = os.getenv("S3_INPUT_BUCKET")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET")
WATERMARK_TEXT = os.getenv("WATERMARK_TEXT", "WATERMARK")

# Helper: crear watermark y serializar como PNG base64
def crear_watermark_serial():
    wm = np.zeros((40, 200, 3), dtype=np.uint8)
    cv2.putText(wm, WATERMARK_TEXT, (5, 25),
                cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 255, 255), 2)
    _, buf = cv2.imencode(".png", wm)
    return base64.b64encode(buf).decode()

def deserializar_np(b64str):
    data = base64.b64decode(b64str)
    arr = np.frombuffer(data, dtype=np.uint8)
    return cv2.imdecode(arr, cv2.IMREAD_COLOR)

# función que ejecutará cada proceso hijo
def procesar_frame(args):
    idx, frame_bytes, watermark_b64, s3_bucket, video_name  = args

    # crear cliente boto3 dentro del hijo
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))

    # reconstruir imagen desde bytes
    frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
    watermark = deserializar_np(watermark_b64)

    # procesar: gris
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    gray = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

    # añadir watermark (top-left)
    h, w = watermark.shape[:2]
    gray[10:10+h, 10:10+w] = watermark

    # codificar a PNG en memoria
    _, buf = cv2.imencode(".png", gray)
    safe_video = os.path.splitext(os.path.basename(video_name))[0]
    key = f"{safe_video}_frame_{idx:06d}.png"

    # subir con put_object desde bytes
    s3.put_object(Bucket=s3_bucket, Key=key, Body=buf.tobytes(), ContentType="image/png")

    return key

def procesar_video(video_key):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    local_in = "/tmp/input_video.mp4"

    print(f"[WORKER] Descargando s3://{S3_INPUT_BUCKET}/{video_key} ...")
    s3.download_file(S3_INPUT_BUCKET, video_key, local_in)
    print("[WORKER] Descargado.")

    cap = cv2.VideoCapture(local_in)
    frames = []
    idx = 0

    # extraer frames y serializar
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        idx += 1
        # redimensionar para controlar tamaño
        frame = cv2.resize(frame, (640, 480))
        ok, enc = cv2.imencode(".jpg", frame)
        frames.append((idx, enc.tobytes()))
    cap.release()
    print(f"[WORKER] Frames extraídos: {len(frames)}")

    # preparar watermark serializada
    watermark_b64 = crear_watermark_serial()

    # PREPARAR args: ahora incluimos video_key para que cada hijo sepa el nombre del video
    args = [(i, fb, watermark_b64, S3_OUTPUT_BUCKET, video_key) for (i, fb) in frames]

    workers = max(1, min(cpu_count(), 4))  # limitar a 4 por contenedor por defecto
    print(f"[WORKER] Lanzando Pool con {workers} procesos")
    with Pool(processes=workers) as pool:
        # procesar_frame debe aceptar (idx, frame_bytes, watermark_b64, s3_bucket, video_name)
        results = list(pool.imap_unordered(procesar_frame, args))

    print(f"[WORKER] Subidos {len(results)} frames a s3://{S3_OUTPUT_BUCKET}")
    # opcional: borrar archivo local
    try:
        os.remove(local_in)
    except Exception:
        pass


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        video = data.get("video")
        print(f"[WORKER] Mensaje recibido: {video}")
        procesar_video(video)
    except Exception as e:
        print("[WORKER] ERROR processing message:", e)
    ch.basic_ack(method.delivery_tag)

def connect_and_consume():
    while True:
        try:
            print("[WORKER] Intentando conectar a RabbitMQ...")
            params = pika.URLParameters(RABBIT_URL)
            conn = pika.BlockingConnection(params)
            channel = conn.channel()
            channel.queue_declare(queue="videos", durable=True)
            print("[WORKER] Conectado. Esperando mensajes...")
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="videos", on_message_callback=callback)
            channel.start_consuming()
        except Exception as e:
            print("[WORKER] RabbitMQ no listo o error, reintentando en 5s...", e)
            time.sleep(5)

if __name__ == "__main__":
    connect_and_consume()
