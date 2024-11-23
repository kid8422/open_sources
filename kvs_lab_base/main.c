#include "kvs.h"

node_t* create_node(int level, const char* key, const char* value) {
    node_t* node = (node_t*)malloc(sizeof(node_t));
    strcpy(node->key, key);
    node->value = strdup(value);
    node->forward = (node_t**)malloc((level + 1) * sizeof(node_t*));
	// log(n)을 보장하기 위해서 이중 포인터를 이용해 다음 포인터를 가리킴

    for (int i = 0; i <= level; i++) {
        node->forward[i] = NULL;
    }
    return node;
}

int random_level() {
    int level = 0;
    while (((double)rand() / RAND_MAX) < P && level < MAX_LEVEL - 1) {
        level++;
    }
    return level;
}

int main() {
    // 1. 처음 open에는 인자를 안 넣어주고, do_recovery를 실행하지 않음
    clock_t start_time, end_time;
    double recovery_time;

    start_time = clock();
    kvs_t* kvs = kvs_open(NULL);
    end_time = clock();

    recovery_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("첫 번째 kvs_open 실행 시간: %f 초\n", recovery_time);

    if (!kvs) {
        printf("Failed to open kvs\n");
        return -1;
    }

    // KVS 상태 출력 (첫 번째 open 후)
    printf("Initial kvs items: %d\n", kvs->items);

    // 2. trc 파일을 불러와서 워크로드 실행
    FILE* input = fopen("cluster004.trc", "r");
    if (!input) {
        printf("Failed to open cluster004.trc\n");
        kvs_close(kvs);
        return -1;
    }

    char operation[4]; // 'set'만 사용
    char key[100];
    char value[5000];
    int total_set_operations = 0;

    clock_t workload_start, workload_end;
    double workload_time;

    workload_start = clock();

    while (fscanf(input, "%3[^,],%99[^,],%4999[^\n]\n", operation, key, value) != EOF) {
        if (strcmp(operation, "set") == 0) {
            put(kvs, key, value);
            total_set_operations++;
        }
    }

    workload_end = clock();
    workload_time = ((double)(workload_end - workload_start)) / CLOCKS_PER_SEC;
    printf("워크로드 실행 시간: %f 초\n", workload_time);

    fclose(input);

    // KVS 상태 출력 (워크로드 실행 후)
    printf("Total set operations: %d\n", total_set_operations);
    printf("Total unique keys stored: %d\n", kvs->items);

    // 3. do_snapshot으로 저장
    double snapshot_time;
    start_time = clock();
    do_snapshot(kvs);
    end_time = clock();
    snapshot_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("do_snapshot 실행 시간: %f 초\n", snapshot_time);

    // KVS 닫기
    kvs_close(kvs);

    // 4. open에 kvs.img를 인자로 주고 kvs를 생성하면서 불러옴
    start_time = clock();
    kvs_t* kvs_loaded = kvs_open("kvs.img");
    end_time = clock();
    recovery_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("두 번째 kvs_open (복구) 실행 시간: %f 초\n", recovery_time);

    if (!kvs_loaded) {
        printf("Failed to open kvs with kvs.img\n");
        return -1;
    }

    // KVS 상태 출력 (두 번째 open 후)
    printf("Loaded kvs items: %d\n", kvs_loaded->items);

    // KVS 닫기
    kvs_close(kvs_loaded);

    return 0;
}