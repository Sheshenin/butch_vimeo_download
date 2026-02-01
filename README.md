# Vimeo Showcase Downloader

Веб-приложение для пакетного скачивания видео из Vimeo showcase.

## Возможности

- Скачивание всех видео из Vimeo showcase по одной ссылке
- Поддержка showcase с паролем
- Прогресс скачивания в реальном времени (SSE)
- Скачивание готовых файлов через браузер
- Темный интерфейс

## Запуск

### Локально

```bash
pip install -r requirements.txt
python app.py
```

Откройте http://localhost:5000

### Docker

```bash
docker build -t vimeo-dl .
docker run -p 5000:5000 -v ./downloads:/app/downloads vimeo-dl
```

## Требования

- Python 3.10+
- ffmpeg (для объединения аудио/видео потоков)
- yt-dlp

## Использование

1. Откройте http://localhost:5000
2. Вставьте ссылку на showcase (например `https://vimeo.com/showcase/12345678`)
3. Введите пароль, если showcase защищен
4. Нажмите «Начать скачивание»
5. Дождитесь завершения и скачайте файлы
