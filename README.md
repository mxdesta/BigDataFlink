# Лабораторная работа №3: Streaming Processing с Apache Flink

## Запуск

```bash
docker-compose down
docker-compose build
docker-compose up -d
```

Подождать 30-60 секунд, затем запустить Flink job:
```bash
docker exec -it jobmanager flink run -py /opt/flink/jobs/streaming_etl.py -d
```

## Проверка

```bash
docker exec -it postgres psql -U flink_user -d flink_db -c "SELECT COUNT(*) FROM fact_sales;"
```
так можем несколько раз выполнить запрос выше, увидим как количество записей постепенно растет

Flink UI: http://localhost:8081
