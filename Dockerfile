FROM python:3.11-slim

WORKDIR /app

# Сначала зависимости — кешируется отдельным слоем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код
COPY . .

# Незабуферизированный stdout — логи появляются сразу
ENV PYTHONUNBUFFERED=1

CMD ["python", "u.py"]
