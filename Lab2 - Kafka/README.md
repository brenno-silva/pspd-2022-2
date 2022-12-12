# Lab Kafka
## 1. Iniciar Docker
`sudo docker compose up -d`

## 2. Compilar arquivos
`make`

## 3. Rodar Producer
`./producer labkafka config.ini`

## 4. Rodar Consumer
`./consumer labkafka config.ini`

## Comando que podem ser úteis
### Entrar no bash do kafka
`sudo docker exec -it lab2-kafka-kafka-1-1 bash`

### Docker Bash
`kafka-console-producer --broker-list localhost:9092 --topic labkafka`

### Excluir tópico
`kafka-topics --delete --bootstrap-server localhost:9092 --topic labkafka`