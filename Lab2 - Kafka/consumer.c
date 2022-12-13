// Lab02 – Construindo aplicação utilizando Broker
// Brenno Oliveira Silva - 190025379, Victor Souza Dantas Martins Lima - 190044403

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <librdkafka/rdkafka.h>

#include "common.h"

#define MAX_CHAR 50

struct user
{
        char *name;
        int sum;
};

typedef struct
{
        char *palavra;
        int qtd;
} palavras;

#define TRACK_USER_CNT 4
static struct user users[TRACK_USER_CNT];

static void dr_cb(rd_kafka_t *rk,
                  const rd_kafka_message_t *rkmessage, void *opaque)
{
        int *delivery_counterp = (int *)rkmessage->_private; /* V_OPAQUE */

        if (rkmessage->err)
        {
                fprintf(stderr, "Delivery failed for message %.*s: %s\n",
                        (int)rkmessage->len, (const char *)rkmessage->payload,
                        rd_kafka_err2str(rkmessage->err));
        }
        else
        {
                fprintf(stderr,
                        "Message delivered to %s [%d] at offset %" PRId64
                        " in %.2fms: %.*s\n",
                        rd_kafka_topic_name(rkmessage->rkt),
                        (int)rkmessage->partition,
                        rkmessage->offset,
                        (float)rd_kafka_message_latency(rkmessage) / 1000.0,
                        (int)rkmessage->len, (const char *)rkmessage->payload);
                (*delivery_counterp)++;
        }
}

static int run_producer(const char *topic, int msgcnt,
                        rd_kafka_conf_t *conf, const char *user, char *str)
{
        rd_kafka_t *rk;
        char errstr[512];
        int i;
        int delivery_counter = 0;

        /* Set up a delivery report callback that will be triggered
         * from poll() or flush() for the final delivery status of
         * each message produced. */
        rd_kafka_conf_set_dr_msg_cb(conf, dr_cb);

        /* Create producer.
         * A successful call assumes ownership of \p conf. */
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
        printf(str);
        if (!rk)
        {
                fprintf(stderr, "Failed to create producer: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Create the topic. */
        if (create_topic(rk, topic, 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }

        for (i = 0; i < msgcnt; i++)
        {
                rd_kafka_resp_err_t err;

                fprintf(stderr, "Producing message #%d to %s: %s\n", i, topic, user);

                /* Asynchronous produce */
                err = rd_kafka_producev(
                    rk,
                    RD_KAFKA_V_TOPIC(topic),
                    RD_KAFKA_V_KEY(user, strlen(user)),
                    RD_KAFKA_V_VALUE(str, strlen(str)),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_OPAQUE(&delivery_counter),
                    RD_KAFKA_V_END);

                if (err)
                {
                        fprintf(stderr, "Produce failed: %s\n",
                                rd_kafka_err2str(err));
                        break;
                }

                /* Poll for delivery report callbacks to know the final
                 * delivery status of previously produced messages. */
                rd_kafka_poll(rk, 0);
        }

        rd_kafka_flush(rk, 15 * 1000);

        /* Destroy the producer instance. */
        rd_kafka_destroy(rk);

        fprintf(stderr, "%d/%d messages delivered\n",
                delivery_counter, msgcnt);

        return 0;
}

static struct user *find_user(const char *name, size_t namelen)
{
        int i;

        for (i = 0; i < TRACK_USER_CNT; i++)
        {
                if (!users[i].name)
                {
                        /* Free slot, populate */
                        users[i].name = strndup(name, namelen);
                        users[i].sum = 0;
                        return &users[i];
                }
                else if (!strncmp(users[i].name, name, namelen))
                        return &users[i];
        }

        return NULL; /* No free slots */
}

static int handle_message(rd_kafka_message_t *rkm)
{
        const char *expected_user = "consumer";
        struct user *user;
        int i;

        if (!rkm->key)
                return 0;

        if (!(user = find_user(rkm->key, rkm->key_len)))
                return 0;

        if (rkm->key_len != strlen(expected_user) ||
            strncmp(rkm->key, expected_user, rkm->key_len))
                return 0;

        return 0;
}

char *contaPalavra(char *argp)
{
        char *aux = argp;

        char *str = malloc(sizeof(char) * 30000);
        char *palavra = malloc(sizeof(char) * MAX_CHAR);
        int qtdPalavras = 0;
        palavras *contador = malloc(sizeof(palavras) * 3);

        contador[0].palavra = "Menor que 6";
        contador[0].qtd = 0;

        contador[1].palavra = "Entre 6 e 10";
        contador[1].qtd = 0;

        contador[2].palavra = "Total";
        contador[2].qtd = 0;

        for (int i = 0, j = 0; i < strlen(aux); i++)
        {
                for (j = 0; aux[i] != ' ' && aux[i] != '\0'; i++, j++)
                {
                        palavra[j] = aux[i];
                        if (aux[i + 1] == ' ' || aux[i+1] == '\0')
                        {
                                palavra[j + 1] = '\0';

                                int tamPalavra = strlen(palavra);

                                if (tamPalavra < 6)
                                        contador[0].qtd++;
                                else if (tamPalavra >= 6 && tamPalavra <= 10)
                                        contador[1].qtd++;

                                contador[2].qtd++;
                        }
                }
        }

        sprintf(str, "%d %d %d\n", contador[0].qtd, contador[1].qtd, contador[2].qtd);

        return str;
}

static int run_consumer(const char *topic, rd_kafka_conf_t *conf, const char *config_file)
{
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *topics;
        int i;
        char *str = malloc(sizeof(char *) * 5000);

        rd_kafka_conf_set(conf, "group.id", "cloud-example-c", NULL, 0);

        /* If there is no committed offset for this group, start reading
         * partitions from the beginning. */
        rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", NULL, 0);

        /* Disable ERR__PARTITION_EOF when reaching end of partition. */
        rd_kafka_conf_set(conf, "enable.partition.eof", "false", NULL, 0);

        /* Create consumer.
         * A successful call assumes ownership of \p conf. */
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (!rk)
        {
                fprintf(stderr, "Failed to create consumer: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Redirect all (present and future) partition message queues to the
         * main consumer queue so that they can all be consumed from the
         * same consumer_poll() call. */
        rd_kafka_poll_set_consumer(rk);

        /* Create subscription list.
         * The partition will be ignored by subscribe() */
        topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(topics, topic,
                                          RD_KAFKA_PARTITION_UA);

        /* Subscribe to topic(s) */
        fprintf(stderr,
                "Subscribed to %s, waiting for assignment and messages...\n"
                "Press Ctrl-C to exit.\n",
                topic);
        err = rd_kafka_subscribe(rk, topics);
        rd_kafka_topic_partition_list_destroy(topics);

        if (err)
        {
                fprintf(stderr, "Subscribe(%s) failed: %s\n",
                        topic, rd_kafka_err2str(err));
                rd_kafka_destroy(rk);
                return -1;
        }

        /* Consume messages */
        while (run)
        {
                rd_kafka_message_t *rkm;

                /* Poll for a single message or an error event.
                 * Use a finite timeout so that Ctrl-C (run==0) is honoured. */
                rkm = rd_kafka_consumer_poll(rk, 1000);
                if (!rkm)
                        continue;

                if (rkm->err)
                {
                        /* Consumer error: typically just informational. */
                        fprintf(stderr, "Consumer error: %s\n",
                                rd_kafka_message_errstr(rkm));
                }
                else
                {
                        /* Proper message */
                        fprintf(stderr,
                                "Received message on %s [%d] "
                                "at offset %" PRId64 ": %.*s\n",
                                rd_kafka_topic_name(rkm->rkt),
                                (int)rkm->partition, rkm->offset,
                                (int)rkm->len, (const char *)rkm->payload);
                        handle_message(rkm);
                        str = contaPalavra(rkm->payload);
                        rd_kafka_conf_t *conf2;
                        if (!(conf2 = read_config(config_file)))
                                return 1;

                        if (run_producer("labkafkaresult", 1, conf2, "consumer", str) == -1)
                                return 1;
                }

                rd_kafka_message_destroy(rkm);
        }

        /* Close the consumer to have it gracefully leave the consumer group
         * and commit final offsets. */
        rd_kafka_consumer_close(rk);

        /* Destroy the consumer instance. */
        rd_kafka_destroy(rk);

        for (i = 0; i < TRACK_USER_CNT; i++)
                if (users[i].name)
                        free(users[i].name);


        return 0;
}

int main(int argc, char **argv)
{
        const char *topic;
        const char *config_file;
        const char *user = "consumer";
        rd_kafka_conf_t *conf;

        if (argc != 3)
        {
                fprintf(stderr, "Usage: %s <topic> <config-file>\n", argv[0]);
                exit(1);
        }

        topic = argv[1];
        config_file = argv[2];

        if (!(conf = read_config(config_file)))
                return 1;

        if (run_consumer(topic, conf, config_file) == -1)
                return 1;

        return 0;
}
