#include "kvs.h"

int put(kvs_t* kvs, const char* key, const char* value) {
    node_t* update[MAX_LEVEL];
    node_t* current = kvs->header;

    // 삽입 위치를 찾기 위한 탐색
    for (int i = kvs->level; i >= 0; i--) {
        while (current->forward[i] != NULL && strcmp(current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    current = current->forward[0];

    // 이미 존재하는 키인 경우 값을 업데이트
    if (current != NULL && strcmp(current->key, key) == 0) {
        free(current->value);
        current->value = strdup(value);
        return 0; // 기존 키 업데이트
    }

    // 새로운 노드 삽입
    int new_level = random_level();
    if (new_level > kvs->level) {
        for (int i = kvs->level + 1; i <= new_level; i++) {
            update[i] = kvs->header;
        }
        kvs->level = new_level;
    }

    node_t* new_node = create_node(new_level, key, value);
    for (int i = 0; i <= new_level; i++) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }

    kvs->items++; // 새로운 키 삽입 시 items 증가
    return 0;
}
