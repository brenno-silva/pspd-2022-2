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
#include <signal.h>
#include <string.h>
#include <stdlib.h>

#include <librdkafka/rdkafka.h>

#include "common.h"

struct user
{
        char *name;
        int sum;
};

#define TRACK_USER_CNT 4
static struct user users[TRACK_USER_CNT];

/**
 * @brief Delivery report callback, triggered by from poll() or flush()
 *        once for each produce():ed message to propagate its final delivery status.
 *
 *        A non-zero \c rkmessage->err indicates delivery failed permanently.
 */
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

static int run_consumer(const char *topic, rd_kafka_conf_t *conf, const char *config_file, int qtdWorkers)
{
        rd_kafka_t *rk;
        char errstr[512];
        rd_kafka_resp_err_t err;
        rd_kafka_topic_partition_list_t *topics;
        int i;
        char *str = malloc(sizeof(char *) * 5000);
        int qtd6 = 0, qtd610 = 0, qtdTotal = 0, msgAtual = 0;

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
                        handle_message(rkm);
                        strcpy(str, rkm->payload);

                        char *palavra = malloc(sizeof(char) * 500);

                        int pos = 0;
                        for (int i = 0, j = 0; i < strlen(str); i++)
                        {
                                for (j = 0; str[i] != ' ' && str[i] != '\0'; i++, j++)
                                {
                                        palavra[j] = str[i];
                                        if (str[i + 1] == ' ' || str[i + 1] == '\0')
                                        {
                                                palavra[j + 1] = '\0';

                                                if (pos == 0)
                                                {
                                                        qtd6 += atoi(palavra);
                                                        pos++;
                                                }
                                                else if (pos == 1)
                                                {
                                                        qtd610 += atoi(palavra);
                                                        pos++;
                                                }
                                                else
                                                {
                                                        qtdTotal += atoi(palavra);
                                                        pos++;
                                                }
                                        }
                                }
                        }
                        msgAtual++;
                }
                rd_kafka_message_destroy(rkm);
                if (msgAtual == qtdWorkers)
                {
                        printf("Resultado\n Menor que 6: %d\n Entre 6 e 10: %d\n Total: %d\n", qtd6, qtd610, qtdTotal);
                        break;
                }
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

static int run_producer(const char *topic, int msgcnt,
                        rd_kafka_conf_t *conf, const char *user, const char *config_file, int qtdWorkers)
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
        if (!rk)
        {
                fprintf(stderr, "Failed to create producer: %s\n", errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
        }

        /* Create the topics. */
        if (create_topic(rk, "labkafka", 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }
        if (create_topic(rk, "labkafka2", 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }
        if (create_topic(rk, "labkafka3", 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }
        if (create_topic(rk, "labkafka4", 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }
        if (create_topic(rk, "labkafka5", 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }
        if (create_topic(rk, "labkafka6", 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }

        FILE *file = fopen("entrada.txt", "r");
        char *textFile1 = 0, *textFile2 = 0, *textFile3 = 0,
             *textFile4 = 0, *textFile5 = 0, *textFile6 = 0, *textFile7 = 0;
        long fileLength;

        if (file == NULL)
        {
                fprintf(stderr, "Erro ao abrir arquivo\n");
                return 0;
        }

        fseek(file, 0, SEEK_END);
        fileLength = ftell(file);
        fseek(file, 0, SEEK_SET);
        textFile1 = malloc(fileLength);
        textFile2 = malloc(fileLength / 2);
        textFile3 = malloc(fileLength / 2);
        textFile4 = malloc(fileLength / 2);
        textFile5 = malloc(fileLength / 2);
        textFile6 = malloc(fileLength / 2);
        textFile7 = malloc(fileLength / 2);

        if (textFile1)
                fread(textFile1, 1, fileLength, file);

        fclose(file);

        /* Produce messages */
        for (i = 0; run && i < msgcnt; i++)
        {
                rd_kafka_resp_err_t err;

                fprintf(stderr, "Producing message #%d to %s: %s\n", i, topic, user);

                /* Asynchronous produce */
                int i = 0;

                switch (qtdWorkers)
                {
                case 1:
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile1, strlen(textFile1)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        break;
                case 2:
                        for (int i = 0; i < fileLength / 2; i++)
                        {
                                textFile2[i] = textFile1[i];
                        }

                        for (int i = fileLength / 2, j = 0; i < fileLength; i++, j++)
                        {
                                textFile3[j] = textFile1[i];
                        }

                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile2, strlen(textFile2)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka2"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile3, strlen(textFile3)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        break;
                case 4:
                        for (; i < fileLength / 4; i++)
                        {
                                textFile2[i] = textFile1[i];
                        }

                        for (int j = 0; j < fileLength / 4; j++, i++)
                        {
                                textFile3[j] = textFile1[i];
                        }
                        for (int j = 0; j < fileLength / 4; j++, i++)
                        {
                                textFile4[j] = textFile1[i];
                        }

                        for (int j = 0; j < fileLength / 4; i++, j++)
                        {
                                textFile5[j] = textFile1[i];
                        }

                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile2, strlen(textFile2)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka2"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile3, strlen(textFile3)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka3"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile4, strlen(textFile4)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka4"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile5, strlen(textFile5)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        break;
                case 6:
                        for (; i < fileLength / 6; i++)
                        {
                                textFile2[i] = textFile1[i];
                        }

                        for (int j = 0; j < fileLength / 6; j++, i++)
                        {
                                textFile3[j] = textFile1[i];
                        }
                        for (int j = 0; j < fileLength / 6; j++, i++)
                        {
                                textFile4[j] = textFile1[i];
                        }

                        for (int j = 0; j < fileLength / 6; i++, j++)
                        {
                                textFile5[j] = textFile1[i];
                        }
                        for (int j = 0; j < fileLength / 6; j++, i++)
                        {
                                textFile6[j] = textFile1[i];
                        }

                        for (int j = 0; j < fileLength / 6; i++, j++)
                        {
                                textFile7[j] = textFile1[i];
                        }

                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile2, strlen(textFile2)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka2"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile3, strlen(textFile3)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka3"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile4, strlen(textFile4)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka4"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile5, strlen(textFile5)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka5"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile6, strlen(textFile6)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        err = rd_kafka_producev(
                            rk,
                            RD_KAFKA_V_TOPIC("labkafka6"),
                            RD_KAFKA_V_KEY(user, strlen(user)),
                            RD_KAFKA_V_VALUE(textFile7, strlen(textFile7)),
                            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                            RD_KAFKA_V_OPAQUE(&delivery_counter),
                            RD_KAFKA_V_END);
                        break;
                }

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

        if (run)
        {
                /* Wait for outstanding messages to be delivered,
                 * unless user is terminating the application. */
                fprintf(stderr, "Waiting for %d more delivery results\n",
                        msgcnt - delivery_counter);
                rd_kafka_flush(rk, 15 * 1000);
        }

        /* Destroy the producer instance. */
        rd_kafka_destroy(rk);

        rd_kafka_conf_t *conf2;
        if (!(conf2 = read_config(config_file)))
                return 1;

        if (run_consumer("labkafkaresult", conf2, config_file, qtdWorkers) == -1)
                return 1;

        return 0;
}

int main(int argc, char **argv)
{
        const char *topic;
        const char *config_file;
        const char *user;
        rd_kafka_conf_t *conf;

        if (argc != 4)
        {
                fprintf(stderr, "Usage: %s <topic> <config-file> <user>\n", argv[0]);
                exit(1);
        }

        topic = argv[1];
        config_file = argv[2];
        user = argv[3];

        int qtdWorkers = 0;
        printf("Insira a quantidade de Workers:");
        scanf("%d", &qtdWorkers);

        if (!(conf = read_config(config_file)))
                return 1;

        if (run_producer(topic, 1, conf, user, config_file, qtdWorkers) == -1)
                return 1;

        return 0;
}
