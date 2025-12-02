FROM python:3.10
WORKDIR /worker
RUN apt-get update && apt-get install -y ffmpeg libsm6 libxext6
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "worker.py"]
