FROM python:3.11-slim

# ffmpeg нужен faster-whisper для декодирования голосовых
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Сначала зависимости — кешируется отдельным слоем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код
COPY . .

# Незабуферизированный stdout — логи появляются сразу
ENV PYTHONUNBUFFERED=1

CMD ["python", "u.py"]
