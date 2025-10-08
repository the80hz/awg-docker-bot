# Dockerfile для awg-docker-bot (без SSH, только локальный docker)
FROM python:3.13-slim

# Установить необходимые пакеты
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata gcc && \
    rm -rf /var/lib/apt/lists/*

# Установить рабочую директорию
WORKDIR /app

# Копировать исходники
COPY . /app

# Установить зависимости Python
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Сделать скрипты исполняемыми
RUN chmod +x awg/newclient.sh awg/removeclient.sh

# Переменные окружения для корректной работы tzdata
ENV TZ=Europe/Moscow

# Запуск бота
CMD ["python", "awg/bot_manager.py"]
