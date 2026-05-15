Cerber user-bot package
=======================

Содержимое:
- u.py — основной скрипт (ваш загруженный файл)
- session.session — сессия (ВАЖНО: это секретный файл, не делитесь им)
- requirements.txt — зависимости для установки
- runtime.txt — желаемая версия Python (пример для Heroku/Replit)
- Procfile — для Heroku (worker: python u.py)
- start.sh — простой скрипт для запуска

Ошибки PersistentTimestampOutdatedError / Constructor ID 9c974fdf:
- Обновите Telethon: pip install -U telethon
- Скрипт при этой ошибке автоматически сбрасывает сессию и завершает работу — перезапустите: python u.py (потребуется снова ввести код из Telegram).
- Либо сбросьте сессию вручную до запуска: python u.py --reset-session

Быстрый старт (на Linux / VPS):
1) Скопируйте папку на хост или распакуйте zip.
2) Убедитесь, что у вас установлены Python 3.11+ и pip.
3) Установите зависимости:
   pip install -r requirements.txt
4) Запустите:
   python u.py

Примечания по безопасности:
- Файл session.session содержит данные для доступа к вашему аккаунту. Держите его в секрете.
- Не выкладывайте пакет в публичные места (GitHub без .gitignore для session.session и u.py).
- Хосты вроде Replit/Glitch/Heroku могут временно сохранять сессии; подумайте о защите окружения.
- Если кто-то получит доступ к session.session или api_hash — это компрометация аккаунта.

Если хотите, могу добавить Dockerfile или systemd unit — скажите, какой способ хостинга вы планируете.
