FROM python:3.10

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /etl

COPY requirements.txt requirements.txt

COPY . .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install -r requirements.txt

CMD ["python", "main.py"]