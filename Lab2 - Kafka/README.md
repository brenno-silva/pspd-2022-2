# Lab Kafka
## Iniciar Docker
`sudo docker compose up -d`

## Entrar no bash do kafka
`sudo docker exec -it lab2-kafka-kafka-1-1 bash`

## Compilar arquivos
`make`

## Rodar Producer
`./producer labkafka config.ini`

## Rodar Consumer
`./consumer labkafka config.ini`

## Docker Bash
`kafka-console-producer --broker-list localhost:19092 --topic labkafka`

## Excluir tópico
`kafka-topics --delete --bootstrap-server localhost:19092 --topic labkafka`