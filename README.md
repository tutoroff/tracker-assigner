# Yandex Tracker Auto-Assigner

Сервис автоматического распределения задач в Яндекс Трекере. Работает как единый бинарник на Go, держит локальное состояние в SQLite (режим WAL) и не требует внешних баз данных вроде PostgreSQL или Redis.

---

## Что делает сервис

Сервис снимает с тимлида или дежурного рутину ручной сортировки задач. Входящий вебхук из Трекера проходит через цепочку правил маршрутизации, сервис находит свободного специалиста на смене и сразу назначает исполнителя через REST API.

- **Два режима распределения:** честный `round_robin` по кругу и `load_based` — на того, у кого прямо сейчас меньше всего открытых задач.
- **WIP-лимиты и график смен.** У каждого дежурного задан потолок одновременных задач и персональное расписание с таймзоной (хоть посменно, хоть 5/2). Если смена закончилась или сотрудник уже ведет максимум задач, сервис его не трогает.
- **Очередь ожидания (Pending Queue).** Когда все дежурные перегружены или смена еще не началась, задача не теряется — она отправляется в локальную очередь SQLite. Как только кто-то освободит слот или выйдет на смену, задача тут же уходит в работу.
- **DLQ с повторными попытками.** Если упала сеть или Трекер ответил ошибкой 429/5xx, событие попадает в Dead Letter Queue и дожимается экспоненциальным бэкоффом без потери контекста.
- **Синхронизация счетчиков через TQL.** Если задачу закрыли или перекинули руками прямо в интерфейсе Трекера, фоновый воркер через TQL-запрос выравнивает локальные счетчики нагрузки.
- **Горячая перезагрузка.** Меняете `config.yaml` — сервис подхватывает новые правила на лету через `fsnotify`. Перезапускать процесс не нужно.
- **Уведомления в Telegram и алерты.** Сервис умеет слать карточки назначения в рабочие чаты и бить тревогу, если задача застряла в очереди дольше допустимого SLA.
- **Метрики для Prometheus.** Из коробки доступны `/metrics` для построения графиков в Grafana и `/healthz` для проверки живости сервиса.

---

## Архитектура проекта

```text
├── cmd/
│   └── tracker-assigner/
│       └── main.go                  # Точка входа, запуск воркеров и graceful shutdown
├── internal/
│   ├── config/
│   │   ├── config.go                # Чтение YAML, парсинг переменных и валидация
│   │   └── watcher.go               # Отслеживание изменений конфига (fsnotify)
│   ├── service/
│   │   ├── assigner.go              # Пайплайн обработки входящих вебхуков
│   │   ├── balancer.go              # Балансировка Round-Robin и Load-based с WIP-лимитами
│   │   ├── router.go                # Сопоставление очередей, компонентов и тегов
│   │   ├── tracker_client.go        # HTTP-клиент к REST API Яндекс Трекера (v2/v3)
│   │   └── bug_reporter.go          # Автоматическая фиксация сбоев в Трекере
│   ├── storage/
│   │   ├── sqlite.go                # Драйвер SQLite с прагмами WAL и busy_timeout
│   │   ├── repository.go            # Атомарные транзакции очередей и счетчиков
│   │   └── migrations/              # Встроенные SQL-миграции схемы БД
│   ├── transport/
│   │   └── http/
│   │       ├── webhook_handler.go   # Прием вебхуков с проверкой X-Secret-Token
│   │       ├── api_handler.go       # REST API мониторинга очередей и нагрузок
│   │       └── metrics.go           # Метрики Prometheus
│   └── worker/
│       ├── pending_processor.go     # Разбор очереди ожидания при освобождении слотов
│       ├── dlq_processor.go         # Повтор сбойных запросов с экспоненциальной паузой
│       └── state_sync.go            # Периодическая сверка нагрузки с Трекером через TQL
├── pkg/
│   ├── core/                        # Ядро приложения
│   ├── logger/                      # Структурированный JSON/Console логгер (Zap)
│   └── notifier/                    # Отправка алертов и карточек в Telegram
├── config.example.yaml              # Эталонный файл конфигурации с комментариями
├── Dockerfile                       # Минимальный образ на alpine/scratch
├── docker-compose.yml               # Быстрый запуск сервиса в контейнере
└── Makefile                         # Команды сборки, запуска тестов и линтера
```

---

## Быстрый старт

### Требования
* Go 1.22+
* Git

### 1. Запуск из исходников

```bash
# Клонируем репозиторий и переходим в папку проекта:
git clone https://github.com/tutoroff/tracker-assigner.git
cd tracker-assigner

# Создаем файл конфигурации на основе эталона:
cp config.example.yaml config.yaml

# Указываем OAuth-токен и ID организации в config.yaml
# Запускаем сервис:
go run ./cmd/tracker-assigner -config config.yaml
```

### 2. Сборка исполняемого файла

```bash
# Через Makefile:
make build

# Либо напрямую через go build:
go build -ldflags="-s -w" -trimpath -o bin/tracker-assigner ./cmd/tracker-assigner

# Запуск скомпилированного бинарника:
./bin/tracker-assigner -config config.yaml
```

### 3. Запуск в Docker

```bash
docker-compose up -d --build
docker-compose logs -f tracker-assigner
```

---

## Конфигурация (`config.yaml`)

Сервис читает правила из YAML-файла. Любой параметр можно переопределить через переменные окружения — удобно при деплое в Docker или Kubernetes.

### Переменные окружения
* `CONFIG_PATH` — путь к файлу конфигурации (по умолчанию `config.yaml`).
* `SERVER_PORT` — порт HTTP-сервера (перекрывает `server.port`).
* `WEBHOOK_SECRET` — секретный ключ в заголовке `X-Secret-Token`.
* `TRACKER_TOKEN` — OAuth-токен доступа к API Яндекс Трекера.
* `TRACKER_ORG_ID` — ID организации в Яндекс 360 или Яндекс Cloud.
* `TRACKER_IS_CLOUD` — `true`, если организация создана в Cloud (`X-Cloud-Org-ID`).
* `DATABASE_DSN` — путь к файлу базы данных SQLite (по умолчанию `assigner.db`).
* `DEBUG` — `true` для подробного вывода логов уровня DEBUG.

### Пример конфигурации
```yaml
server:
  host: "0.0.0.0"
  port: 8080
  secret_token: "my-secret-token"

tracker:
  base_url: "https://api.tracker.yandex.net"
  token: "OAuth-token"
  org_id: "123456"
  is_cloud_org: false
  timeout: 15s

database:
  dsn: "assigner.db"

routing_rules:
  - id: "rule-billing"
    queue: "SUPPORT"
    components: ["billing", "payments"]
    target_group: "finance_team"

  - id: "rule-vip"
    queue: "SUPPORT"
    tags: ["vip"]
    target_group: "l2_team"

  - id: "rule-default"
    queue: "SUPPORT"
    target_group: "l1_team"

groups:
  l1_team:
    strategy: "round_robin"      # round_robin либо load_based
    max_load_per_user: 5         # Лимит открытых задач на сотрудника
    schedule:
      timezone: "Europe/Moscow"
      work_days: [1, 2, 3, 4, 5] # 1=Пн ... 5=Пт
      work_hours:
        start: "09:00"
        end: "18:00"
    assignees:
      - login: "ivanov"
      - login: "petrov"
```

---

## Настройка Яндекс Трекера

### Создание триггера в очереди
1. В веб-интерфейсе **Яндекс Трекера** откройте нужную очередь (например, `SUPPORT`).
2. Перейдите в раздел **Настройки очереди** -> вкладка **Триггеры** -> нажмите **Создать триггер**.
3. Укажите условия срабатывания:
   * **Событие:** «Создание задачи» (и при необходимости «Изменение задачи»).
   * **Критерий:** `Исполнитель: Не назначен`.
4. Настройте действие:
   * **Действие:** «Отправить HTTP-запрос».
   * **Метод:** `POST`.
   * **Адрес:** `https://<ваш_домен_или_ip>/webhook`.
   * **Заголовки:**
     * `Content-Type`: `application/json`
     * `X-Secret-Token`: значение из поля `server.secret_token` в вашем `config.yaml`.
   * **Тело запроса:** стандартный JSON вебхука Трекера.

### Локальная проверка через туннель (ngrok)

Проверить интеграцию с живым Трекером на ноутбуке можно через ngrok:

```bash
# 1. Запустите сервис на порту 8080:
go run ./cmd/tracker-assigner -config config.yaml

# 2. Во второй вкладке терминала откройте туннель:
ngrok http 8080

# 3. Скопируйте HTTPS-адрес и вставьте его в настройки триггера Трекера:
# https://xxxx.ngrok-free.app/webhook
```

Создайте тестовую задачу в очереди. Сервис получит хук и запишет события в лог:
```json
{"level":"INFO","timestamp":"2026-09-13T10:00:00Z","msg":"Received tracker webhook","issue_key":"SUPPORT-101","queue":"SUPPORT","status":"open"}
{"level":"INFO","timestamp":"2026-09-13T10:00:00Z","msg":"Assigning ticket to candidate","issue_key":"SUPPORT-101","assignee":"ivanov","group":"l1_team"}
```

---

## Мониторинг и REST API

### Проверка работоспособности
* `GET /healthz` — отдает `{"status":"ok"}` при исправном SQLite и рабочем цикле.

### Prometheus-метрики
* `GET /metrics` — сбор метрик для Prometheus:
  * `tracker_assigner_tickets_assigned_total` — общее число распределенных задач.
  * `tracker_assigner_pending_queue_size` — количество задач в очереди ожидания.
  * `tracker_assigner_dlq_size` — число упавших сообщений в Dead Letter Queue.
  * `tracker_assigner_webhook_requests_total` — счетчик принятых вебхуков.
  * `tracker_assigner_webhook_duration_seconds` — гистограмма времени обработки.

### Внутренний API состояния
* `GET /api/v1/status` — сводка по сервису и счетчики очередей.
* `GET /api/v1/pending?limit=50&offset=0` — список задач, ждущих освобождения исполнителей.
* `GET /api/v1/dlq?limit=50&offset=0` — ошибочные вебхуки для ручного разбора.
* `GET /api/v1/loads?group=l1_team` — текущая нагрузка сотрудников и время крайнего назначения.

---

## Тестирование

Тестовый набор изолирован от сети и проверяет логику на in-memory SQLite:

```bash
# Запуск всех тестов:
go test -v ./...

# Прогон с отчетом о покрытии кода:
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

---

## Лицензия

Проект поставляется под лицензией Apache 2.0.
