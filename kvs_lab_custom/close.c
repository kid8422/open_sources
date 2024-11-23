#include "kvs.h"

int kvs_close(kvs_t* kvs) {
    node_t* current = kvs->header;
    while (current) {
        node_t* next = current->forward[0];
        free(current->value);
        free(current->forward);
        free(current);
        current = next;
    }
    free(kvs);
    return 0;
}
