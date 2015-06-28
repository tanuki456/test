#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <hiredis.h>

#include "cJSON.c"

#define BUFFER_LENGTH (1024 * 1024)

void hset_int(redisContext *c, char *hash_name, char *hash_key, int hash_value) {
    redisReply *reply;
    reply = redisCommand(c, "HSET %s %s %d", hash_name, hash_key, hash_value);
    freeReplyObject(reply);
}

void hset_string(redisContext *c, char *hash_name, char *hash_key, char *hash_value) {
    redisReply *reply;
    reply = redisCommand(c, "HSET %s %s %s", hash_name, hash_key, hash_value);
    freeReplyObject(reply);
}

void hincby(redisContext *c, char *hash_name, char *hash_key, int increment) {
    redisReply *reply;
    reply = redisCommand(c, "HINCRBY %s %s %d", hash_name, hash_key, increment);
    freeReplyObject(reply);
}

void hmax(redisContext *c, char *hash_name, char *hash_key, int value) {
    redisReply *reply;
    reply = redisCommand(c, "HGET %s %s", hash_name, hash_key);
    if (reply->integer >= value){
        return;
    }
    freeReplyObject(reply);
    hset_int(c, hash_name, hash_key, value);
}

int main(int argc, char **argv) {
    redisContext *c;
    FILE *file;
    const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? atoi(argv[2]) : 6379;
    const char *filename = (argc > 3) ? argv[3] : NULL;
    char buffer[BUFFER_LENGTH];

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            printf("Connection error: %s\n", c->errstr);
            redisFree(c);
        } else {
            printf("Connection error: can't allocate redis context\n");
        }
        exit(1);
    }

    if (!filename) {
        printf("Usage: %s host port json_path\n", argv[0]);
        exit(1);
    }

    file = fopen(filename, "rb");
    while (fgets(buffer, BUFFER_LENGTH, file)) {
        cJSON *json;
	
	json = cJSON_Parse(buffer);
	if (!json) {
            printf("Error before: [%s]\n", cJSON_GetErrorPtr());
        }
	else
	{
            cJSON *obj = cJSON_GetObjectItem(json, "hoge");
            if (obj) {
                hset_int(c, "table", "hoge", obj->valueint);
                hincby(c, "table", "huga", obj->valueint);
                hmax(c, "table", "huge", obj->valueint);
            }
	    cJSON_Delete(json);
	}
    }

    redisFree(c);
    fclose(file);
    return 0;
}
