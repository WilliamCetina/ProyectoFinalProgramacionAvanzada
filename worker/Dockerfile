FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    ffmpeg libsm6 libxext6 libgl1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /worker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "worker.py"]
