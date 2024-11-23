#include "kvs.h"

int put(kvs_t* kvs, const char* key, const char* value) {
    node_t* update[MAX_LEVEL];
    node_t* current = kvs->header;

    // 삽입 위치를 찾기 위한 탐색
    for (int i = kvs->level; i >= 0; i--) {
        while (current->forward[i] && custom_strcmp(current->forward[i]->key, key) < 0) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    current = current->forward[0];

    // 이미 존재하는 키인 경우 값을 업데이트
    if (current && custom_strcmp(current->key, key) == 0) {
        free(current->value);
        current->value = custom_strdup(value);
        if (!current->value) return -1; // 메모리 할당 실패 처리
        return 0;
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
    if (!new_node) return -1; // 메모리 할당 실패 처리

    for (int i = 0; i <= new_level; i++) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }

    kvs->items++;
    return 0;
}