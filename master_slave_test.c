#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>
#include <assert.h>

int main(int argc, char **argv) {
    unsigned int j;
    redisContext *c_master;
    redisContext *c_slave[100];
    redisReply *reply;
    char *hostname_master, *hostname_slave[100];
    int port_master, port_slave[100];

    hostname_master = "127.0.0.1";
    port_master = 9221;
    int slave_count = 0;
    if (argc == 1) {
        printf("parameter error!\n");
        return -1;
    }
    if (argc >= 3) {
        hostname_master = argv[1];
        port_master = atoi(argv[2]);
    }
    for (int i = 3; i < argc; i += 2) {
        hostname_slave[slave_count] = argv[i];
        port_slave[slave_count] = atoi(argv[i + 1]);
        slave_count ++;
    }
    if (slave_count <= 0) {
        printf("slave count is not zero!\n");
        return -1;
    }
    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c_master = redisConnectWithTimeout(hostname_master, port_master, timeout);
    if (c_master == NULL || c_master->err) {
        if (c_master) {
            printf("Connection master error: %s\n", c_master->errstr);
            redisFree(c_master);
        } else {
            printf("Connection master error: can't allocate redis context\n");
        }
        exit(1);
    }
    for (int i = 0; i < slave_count; i ++) {
        c_slave[i] = redisConnectWithTimeout(hostname_slave[i], port_slave[i], timeout);
        if (c_slave[i] == NULL || c_slave[i]->err) {
            if (c_slave[i]) {
                printf("Connection slave error: %s\n", c_slave[i]->errstr);
                redisFree(c_slave[i]);
            } else {
                printf("Connection slave error: can't allocate redis context\n");
            }
            exit(1);
        }
    }

    /* PING server */
    reply = (redisReply*)redisCommand(c_master, "PING");
    printf("Master PING: %s\n", reply->str);
    freeReplyObject(reply);
    for (int i = 0; i < slave_count; i ++) {
        reply = (redisReply*)redisCommand(c_slave[i], "PING");
        printf("Slave %d PING: %s\n", i, reply->str);
        freeReplyObject(reply);
    }
    printf("basic test----------------\n");
    {
        reply = (redisReply*)redisCommand(c_master,  "set a b");
        freeReplyObject(reply);

        // read form slave
        for (int i = 0; i < slave_count; i ++) {
            int retry_count = 10;
            int len;
            char buffer[1024];
            do {
                if (retry_count == 0) {
                    printf("i:%d\n", i);
                    assert(retry_count > 0 && i >= 0);
                }
                retry_count --;
                usleep(100000);
                reply = (redisReply*)redisCommand(c_slave[i], "get a");
                len = reply->len;
                memcpy(buffer, reply->str, len);
                freeReplyObject(reply);
            } while (len == 0);
            assert(len == 1);
            assert(memcmp("b", buffer, len) == 0);
        }

        reply = (redisReply*)redisCommand(c_master, "flushdb string");
        freeReplyObject(reply);

        usleep(100000);
        for (int i = 0; i < slave_count; i ++) {
            int retry_count = 10;
            int len;
            do {
                usleep(100000);
                reply = (redisReply*)redisCommand(c_slave[i], "get a");
                len = reply->len;
                freeReplyObject(reply);
                assert(retry_count > 0);
                retry_count --;
            } while (len > 0);
        }
    }
    printf("OK!\n");
    printf("flushall test-------------\n");
    {
        int i;
        for (i = 0; i < 100; i ++) {
            reply = (redisReply*)redisCommand(c_master, "set key%d value%d", i, i);
            freeReplyObject(reply);
            reply = (redisReply*)redisCommand(c_master, "rpush list l%d", i);
            freeReplyObject(reply);
            reply = (redisReply*)redisCommand(c_master, "sadd set s%08d", i);
            freeReplyObject(reply);
            reply = (redisReply*)redisCommand(c_master, "zadd zset %d %d", i, i);
            freeReplyObject(reply);
            reply = (redisReply*)redisCommand(c_master, "hset hash %d %d", i, i);
            freeReplyObject(reply);
        }
        usleep(1000000);
        // string data confirm
        for (int sid = 0; sid < slave_count; sid ++) {
            char buffer[128], value[128];
            {
                for (i = 0; i < 100; i ++) {
                    int retry_count = 10;
                    int len = 0;
                    sprintf(buffer, "value%d", i);
                    do {
                        assert(retry_count > 0);
                        if (retry_count == 10)
                            usleep(10000);
                        else
                            usleep(100000);
                        retry_count --;
                        reply = (redisReply*)redisCommand(c_slave[sid], "get key%d", i);
                        len = reply->len;
                        memcpy(value, reply->str, len);
                        freeReplyObject(reply);
                    } while (len <= 0 || len != strlen(buffer));
                    assert(len == strlen(buffer));
                    assert(memcmp(value, buffer, len) == 0);
                }
            }
            // list data confirm
            {
                for (int i = 0; i < 100; i ++) {
                    int retry_count = 10;
                    int len = 0;
                    sprintf(buffer, "l%d", i);
                    retry_count = 10;
                    do {
                        assert(retry_count > 0);
                        if (retry_count == 10)
                            usleep(10000);
                        else
                            usleep(100000);
                        retry_count --;
                        reply = (redisReply*)redisCommand(c_slave[sid], "lindex list %d", i);
                        len = reply->len;
                        memcpy(value, reply->str, len);
                    } while (len <= 0 || len != strlen(buffer));
                    assert(len == strlen(buffer));
                    assert(memcmp(value, buffer, len) == 0);
                }
            }
            // hash data confirm
            {
                for (int i = 0; i < 100; i ++) {
                    int retry_count = 10;
                    int len = 0;
                    sprintf(buffer, "%d", i);
                    retry_count = 10;
                    do {
                        assert(retry_count > 0);
                        if (retry_count == 10) {
                            usleep(10000);
                        } else {
                            usleep(100000);
                        }
                        retry_count --;
                        reply = (redisReply*)redisCommand(c_slave[sid], "hget hash %d", i);
                        len = reply->len;
                        memcpy(value, reply->str, len);
                    } while (len <= 0 || len != strlen(buffer));
                    assert(len == strlen(buffer));
                    assert(memcmp(value, buffer, len) == 0);
                }
            }
            // set data confirm
            {
                int retry_count = 10;
                do {
                    if (retry_count == 10) {
                        usleep(10000);
                    } else {
                        usleep(100000);
                    }
                    retry_count --;
                    reply = (redisReply*)redisCommand(c_slave[sid], "smembers set");
                } while (reply->type != REDIS_REPLY_ARRAY && retry_count > 0);
                for (int i = 0; i < reply->elements; i ++) {
                    sprintf(buffer, "s%08d", i);
                    assert(reply->element[i]->len == strlen(buffer));
                    assert(memcmp(buffer, reply->element[i]->str, strlen(buffer)) == 0);
                }
                freeReplyObject(reply);
            }
            // zset data confirm
            {
                int retry_count = 10;
                do {
                    if (retry_count == 10) {
                        usleep(10000);
                    } else {
                        usleep(100000);
                    }
                    retry_count --;
                    reply = (redisReply*)redisCommand(c_slave[sid], "zrange zset 0 -1 WITHSCORES");
                } while (reply->type != REDIS_REPLY_ARRAY && retry_count > 0);
                assert(reply->elements % 2 == 0);
                for (int i = 0; i < reply->elements; i += 2) {
                    sprintf(buffer, "%d", i / 2);
                    assert(reply->element[i]->len == strlen(buffer));
                    assert(reply->element[i + 1]->len == strlen(buffer));
                    assert(memcmp(reply->element[i]->str, buffer, strlen(buffer)) == 0);
                    assert(memcmp(reply->element[i+1]->str, buffer, strlen(buffer)) == 0);
                }
                freeReplyObject(reply);
            }
            printf("slave sid:%d pass!\n", sid);
        }
        // flushall all data confirm
        reply = (redisReply*)redisCommand(c_master, "flushall");
        usleep(500000);
        freeReplyObject(reply);
        for (int sid = 0; sid < slave_count; sid ++) {
            int retry_count = 10;
            int len;
            do {
                usleep(10000);
                reply = (redisReply*)redisCommand(c_slave[sid], "keys *");
                len = reply->len;
                freeReplyObject(reply);
            } while (len > 0);
            assert(len == 0);
        }
    }
    printf("OK!\n");

    redisFree(c_master);
    for (int i = 0; i < slave_count; i ++)
        redisFree(c_slave[i]);
    printf("PASS\n");
    return 0;
}
