        # kafka — Продьюсер: send, acks, idempotence

        Homework-шаблон для урока **l3_producer_basics** (Продьюсер: send, acks, idempotence) на платформе Vibe Learn.

        ## Что делать

        Producer benchmark на Go: измерь throughput и p99 latency при трёх конфигурациях acks
(acks=0, acks=1, acks=all). Запусти локальный Kafka через Docker Compose, отправь по
10 000 сообщений в каждом режиме, зафиксируй время. CI проверяет: (1) acks=0 быстрее acks=1
быстрее acks=all (по суммарному времени); (2) ни одна из трёх прогонов не завершилась с ошибками;
(3) общее число залогированных ack совпадает с числом отправленных сообщений для acks=1 и acks=all.

## Контекст (из transfer-задачи урока)

Платёжный сервис пишет события `payment_completed` в Kafka. Требования:
- **SLA по надёжности:** нельзя тихо потерять ни одно платёжное событие.
- **SLA по latency:** p99 ≤ 200ms (от вызова `send()` до подтверждения).
- **Пиковый throughput:** 5 000 событий/сек.

Текущая конфигурация продьюсера:
```
acks=1
batch.size=16384  (16 KB)
linger.ms=0
retries=0
enable.idempotence=false
```

## Recap из урока

- `producer.send()` — **асинхронный** вызов, возвращает Future. Что значит «успех» — определяет `acks`: 0 (fire-and-forget), 1 (leader-ack), all (ISR-ack).
- `acks=0` — самый быстрый и самый опасный режим: тихая потеря данных без единого алерта. Для критических данных — только `acks=all` + `min.insync.replicas=2`.
- `batch.size` — верхняя граница объёма батча по байтам. `linger.ms` — окно ожидания по времени. Вместе они управляют балансом latency ↔ throughput без ущерба для одиночных сообщений.
- `enable.idempotence=true` требует `acks=all` + `retries > 0` + `max.in.flight <= 5`. Гарантирует dedup внутри партиции через PID + sequence number. Не заменяет Kafka Transactions.
- `retries=0` **не** защищает от дублей: потерянный ack при сетевом сбое или ручная переотправка при ошибке дают тот же дубль. Единственная защита — idempotent producer.

        ## Как работать

        1. Платформа Vibe Learn создаёт копию этого репо в твоём GitHub-аккаунте по клику «Начать домашку» на странице урока (через GitHub `/generate`, codecrafters-pattern).
        2. Склонируй копию локально, реализуй TODO в `main.go`, прогони тесты, запушь.
        3. CI (`.github/workflows/ci.yml`) запускает `go vet` + `go test ./...` на каждый push. Платформа слушает результат через webhook от GitHub Actions и обновляет статус домашки на странице урока.

        ## Локальное окружение

        - Go 1.22+
        - Docker + docker-compose — `docker compose -f docker-compose.yml up -d` поднимает 3-нодовый Kafka cluster на портах 9092/9093/9094, использовать в тестах через bootstrap `localhost:9092,localhost:9093,localhost:9094`.

        ## Запуск

        ```bash
        # Поднять локальный Kafka
        docker compose up -d

        # Прогнать тесты (часть из них стартует свой ephemeral testcontainers cluster, часть использует docker-compose выше)
        go test ./...

        # Запустить main (печатает marker; замени stub на реализацию)
        go run .
        ```

        ## Заметка автора

        Это baseline-шаблон, сгенерированный платформой. Бизнес-сущность задачи (что конкретно реализовать в `main.go`, какие тесты сделать строгими) расширяется по ходу итераций — параллельно с углублением теории урока.
