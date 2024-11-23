#include "kvs.h"

int do_snapshot(kvs_t* kvs) {
    FILE* fp = fopen("kvs.img", "w");
    if (!fp) {
        perror("kvs.img 파일 열기에 실패했습니다.");
        return -1;
    }

    node_t* current = kvs->header->forward[0];
    int saved_items = 0;

    while (current) {
        fprintf(fp, "%s\t%s\n", current->key, current->value);
        saved_items++;
        current = current->forward[0];
    }

    fflush(fp);
    int fd = fileno(fp);
    fsync(fd);

    fclose(fp);

    printf("Total items saved in kvs.img: %d\n", saved_items);

    return 0;
}
