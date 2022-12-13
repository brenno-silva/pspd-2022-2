# Lab Kafka

## Pré requisitos
- librdkafka

## 1. Iniciar Docker
`sudo docker compose up`

## 2. Compilar arquivos
`make`

## 3. Rodar Producer
`./producer labkafka config.ini user`

## 4. Rodar Consumer
`./consumer labkafka config.ini`

## Comandos que podem ser úteis
### Entrar no bash do kafka
`sudo docker exec -it kafka bash`

### Docker Bash
`kafka-console-producer --broker-list localhost:9092 --topic labkafka`

### Excluir tópico
`kafka-topics --delete --bootstrap-server localhost:9092 --topic labkafka`

### Listar tópicos
`kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic labkafka --from-beginning --max-messages 100`