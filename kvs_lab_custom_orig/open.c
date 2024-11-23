#include "kvs.h"
#include <time.h>
#include <stdlib.h>

kvs_t* kvs_open(const char* filename) {
    srand(time(NULL)); // 랜덤 레벨 생성을 위한 시드 설정

    kvs_t* kvs = (kvs_t*)malloc(sizeof(kvs_t));
    if (!kvs) {
        return NULL;
    }

    kvs->level = 0;
    kvs->items = 0;
    kvs->header = create_node(MAX_LEVEL - 1, "", ""); // 빈 헤더 노드 생성

    if (filename != NULL) {
        // kvs.img에서 데이터 복구
        do_recovery(kvs, filename);
    }

    return kvs;
}
