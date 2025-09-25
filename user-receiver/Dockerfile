FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends gcc libffi-dev \
  && pip install --upgrade pip \
  && pip install -r requirements.txt \
  && apt-get remove -y gcc \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "3000", "--ws-ping-interval", "0"]
