import pika
import boto3
import cv2
import numpy as np
from multiprocessing import Pool, cpu_count, set_start_method
import os
import json
import base64
import time

try:
    set_start_method("spawn")
except RuntimeError:
    pass

RABBIT_URL = os.getenv("RABBIT_URL", "amqp://guest:guest@rabbitmq:5672/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_INPUT_BUCKET = os.getenv("S3_INPUT_BUCKET")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET")
WATERMARK_TEXT = os.getenv("WATERMARK_TEXT", "WATERMARK")

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

def procesar_frame(args):
    idx, frame_bytes, watermark_b64, s3_bucket, video_name  = args

    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION"))

    frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
    watermark = deserializar_np(watermark_b64)

    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    gray = cv2.cvtColor(gray, cv2.COLOR_GRAY2BGR)

    h, w = watermark.shape[:2]
    gray[10:10+h, 10:10+w] = watermark

    _, buf = cv2.imencode(".png", gray)
    safe_video = os.path.splitext(os.path.basename(video_name))[0]
    key = f"{safe_video}_frame_{idx:06d}.png"

    s3.put_object(Bucket=s3_bucket, Key=key, Body=buf.tobytes(), ContentType="image/png")

    return key

def procesar_video(video_key):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    local_in = "/tmp/input_video.mp4"
    s3.download_file(S3_INPUT_BUCKET, video_key, local_in)
    
    cap = cv2.VideoCapture(local_in)
    frames = []
    idx = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        idx += 1
        frame = cv2.resize(frame, (640, 480))
        ok, enc = cv2.imencode(".jpg", frame)
        frames.append((idx, enc.tobytes()))
    cap.release()
    watermark_b64 = crear_watermark_serial()
    args = [(i, fb, watermark_b64, S3_OUTPUT_BUCKET, video_key) for (i, fb) in frames]

    workers = max(1, min(cpu_count(), 4))
    with Pool(processes=workers) as pool:
        results = list(pool.imap_unordered(procesar_frame, args))

    try:
        os.remove(local_in)
    except Exception:
        pass


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        video = data.get("video")
        procesar_video(video)
    except Exception as e:
        print("[WORKER] ERROR processing message:", e)
    ch.basic_ack(method.delivery_tag)

def connect_and_consume():
    while True:
        try:
            params = pika.URLParameters(RABBIT_URL)
            conn = pika.BlockingConnection(params)
            channel = conn.channel()
            channel.queue_declare(queue="videos", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="videos", on_message_callback=callback)
            channel.start_consuming()
        except Exception as e:
            time.sleep(5)

if __name__ == "__main__":
    connect_and_consume()
