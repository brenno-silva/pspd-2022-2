/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/* Only track the first 4 users seen, for keeping the example simple. */
#define TRACK_USER_CNT 4
static struct user users[TRACK_USER_CNT];

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

palavras *contaPalavra(char *argp)
{
        char *aux = argp;

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
                        if (aux[i + 1] == ' ' || aux[i] == '\0')
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

        for (int i = 0; i < 3; i++)
                printf("%s\t%d\n", contador[i].palavra, contador[i].qtd);

        return contador;
}

static int run_consumer(const char *topic, rd_kafka_conf_t *conf)
{
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *topics;
        int i;

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
                        contaPalavra(rkm->payload);
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

        if (run_consumer(topic, conf) == -1)
                return 1;

        return 0;
}
