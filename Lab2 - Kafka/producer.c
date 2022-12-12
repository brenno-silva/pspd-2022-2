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

static int run_producer(const char *topic, int msgcnt,
                        rd_kafka_conf_t *conf)
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

        /* Create the topic. */
        if (create_topic(rk, topic, 1) == -1)
        {
                rd_kafka_destroy(rk);
                return -1;
        }

        FILE *file = fopen("entrada.txt", "r");
        char *textFile1 = 0, *textFile2 = 0, *textFile3 = 0;
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
        textFile2 = malloc(fileLength/2);
        textFile3 = malloc(fileLength/2);

        if (textFile1)
                fread(textFile1, 1, fileLength, file);

        fclose(file);

        // for (int i = 0; i < fileLength/2; i++)
        // {
        //         textFile2[i] = textFile1[i];
        // }

        // for (int i = fileLength / 2, j = 0; i < fileLength; i++, j++)
        // {
        //         textFile3[j] = textFile1[i];
        // }

        //printf("%s\n", textFile1);

        /* Produce messages */
        for (i = 0; run && i < msgcnt; i++)
        {
                const char *user = "consumer";
                rd_kafka_resp_err_t err;

                fprintf(stderr, "Producing message #%d to %s: %s\n", i, topic, user);

                /* Asynchronous produce */
                err = rd_kafka_producev(
                    rk,
                    RD_KAFKA_V_TOPIC("labkafka2"),
                    RD_KAFKA_V_KEY(user, strlen(user)),
                    RD_KAFKA_V_VALUE(textFile1, strlen(textFile1)),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                    RD_KAFKA_V_OPAQUE(&delivery_counter),
                    RD_KAFKA_V_END);

                // err = rd_kafka_producev(
                //     rk,
                //     RD_KAFKA_V_TOPIC("labkafka"),
                //     RD_KAFKA_V_KEY(user, strlen(user)),
                //     RD_KAFKA_V_VALUE(textFile1, strlen(textFile3)),
                //     RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                //     RD_KAFKA_V_OPAQUE(&delivery_counter),
                //     RD_KAFKA_V_END);
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

        fprintf(stderr, "%d/%d messages delivered\n",
                delivery_counter, msgcnt);

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

        if (run_producer(topic, 1, conf) == -1)
                return 1;

        return 0;
}
