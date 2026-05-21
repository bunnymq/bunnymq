# Demo

0. Очищаем то, что было

```shell
make cluster-down
```

1. Поднимаем кластер

```shell
make cluster-up
```

2. Убеждаемся, что кластер поднялся

```shell
./bunnymq-cli cluster describe --brokers localhost:19091
```

3. Открываем [графану с метриками](http://localhost:3000/d/bunnymq-overview/bunnymq?orgId=1&from=now-1m&to=now)

4. Проверяем есть ли созданные топики

```shell
./bunnymq-cli --brokers localhost:19091 topic list
```

5. Создаем топик

```shell
./bunnymq-cli topic create \
    --brokers localhost:19091 \
    --name events \
    --partitions 1 \
    --replication-factor 3 \
    --retention-ms 86400000 \
    --retention-bytes 1073741824
```

6. Проверяем, создался ли топик на всех 3 брокерах

```shell
./bunnymq-cli --brokers localhost:19091 topic list
./bunnymq-cli --brokers localhost:29091 topic list
./bunnymq-cli --brokers localhost:39091 topic list
```

7. Смотрим метаданные топика

```shell
./bunnymq-cli --brokers localhost:19091 topic describe --name events
```

8. Меняем количество партиций

```shell
./bunnymq-cli --brokers localhost:19091 topic alter-partitions --name events --partitions 6
```

9. Проверяем, что количество партиций поменялось

```shell
./bunnymq-cli --brokers localhost:19091 topic describe --name events
```

10. Сплиттим экран

11. Запускаем одного консьюмера

```shell
./bunnymq-cli --brokers localhost:19091 consume --topic events --group consumer-1
```

12. Запускаем второго консьюмера

```shell
./bunnymq-cli --brokers localhost:19091 consume --topic events --group consumer-1
```

13. Продьюсим сообщения

```shell
./bunnymq-cli --brokers localhost:19091 produce --topic events --value 'Hello' --acks all --key k1
```

14. Закрываем терминалы с консьюмерами

15. Убиваем один брокер

```shell
docker-compose down broker3
```

16. Проверяем, что партиции ребалансировались

```shell
./bunnymq-cli --brokers localhost:19091 topic describe --name events 
```

17. Пробуем продьюсить сообщения

```shell
./bunnymq-cli --brokers localhost:19091 produce --topic events --value 'Hello' --acks all --key k1
```

18. Убиваем еще один брокер

```shell
docker-compose down broker2
```

19. Пробуем записать сообщения, понимаем, что 1 < (N+1)/2

```shell
./bunnymq-cli --brokers localhost:19091 produce --topic events --value 'Hello' --acks all --key k1
```

20. Поднимаем обратно брокер

```shell
docker-compose up broker2 -d
```

21. Пробуем записать сообщения, понимаем, что 2 >= (N+1)/2

```shell
./bunnymq-cli --brokers localhost:19091 produce --topic events --value 'Hello' --acks all --key k1
```
