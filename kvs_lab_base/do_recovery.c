#include "kvs.h"

int do_recovery(kvs_t* kvs, const char* filename) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        perror("kvs.img 파일 열기에 실패했습니다.");
        return -1;
    }

    char key[100];
    char value[5000];
    int recovered_items = 0;

    while (fscanf(fp, "%99[^\t]\t%4999[^\n]\n", key, value) != EOF) {
        put(kvs, key, value);
        recovered_items++;
    }

    fclose(fp);

    printf("Total items recovered from %s: %d\n", filename, recovered_items);

    return 0;
}
