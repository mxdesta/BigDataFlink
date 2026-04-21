# Лабораторная работа №3: Streaming Processing с Apache Flink

Потоковая обработка данных: Kafka → Flink → PostgreSQL (Star Schema)

## Запуск

```bash
docker-compose build
docker-compose up -d
```

ETL автоматически запустится через 45 секунд после старта всех сервисов.

## Проверка результатов

так можем несколько раз выполнить запрос выше, увидим как количество записей постепенно растет

```bash
docker exec -it postgres psql -U flink_user -d flink_db -c "SELECT COUNT(*) FROM fact_sales;"
```

Flink UI: http://localhost:8081

## Остановка

```bash
docker-compose down
```
