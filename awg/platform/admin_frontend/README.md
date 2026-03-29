# Admin Frontend Placeholder

Здесь будет размещен фронтенд админ-панели.

Предлагаемая структура:
- src/pages — страницы авторизации, список серверов, список профилей
- src/features/users — фильтры, группировка по owner_id, действия над профилями
- src/features/servers — управление серверами и SSH параметрами

План интеграции:
1. Подключить фронтенд к API в awg/platform/api/main.py.
2. Добавить auth middleware в API.
3. Перенести админские Telegram-сценарии в application services и переиспользовать их в UI.
