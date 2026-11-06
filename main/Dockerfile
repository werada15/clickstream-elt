FROM python:3.11-slim

WORKDIR /app

COPY main/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY main/ /app/

CMD ["python", "extract.py"]
