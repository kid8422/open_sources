#include "kvs.h"

int do_snapshot(kvs_t* kvs) {
    int fd = open("kvs.img", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        write(2, "Failed to open kvs.img for snapshot\n", 36);
        return -1;
    }

    node_t* current = kvs->header->forward[0];
    int saved_items = 0;

    while (current) {
        uint32_t key_len = (uint32_t)custom_strlen(current->key);
        uint32_t value_len = (uint32_t)custom_strlen(current->value);

        if (value_len > 5000) {
            write(2, "Value length too long\n", 22);
            close(fd);
            return -1;
        }

        // 키 길이 쓰기
        write(fd, &key_len, sizeof(uint32_t));
        // 키 쓰기
        write(fd, current->key, key_len);
        // 값 길이 쓰기
        write(fd, &value_len, sizeof(uint32_t));
        // 값 쓰기
        write(fd, current->value, value_len);

        saved_items++;
        current = current->forward[0];
    }

    fsync(fd); // 데이터를 디스크에 동기화
    close(fd);

    // 저장된 아이템 수 출력
    char buffer[100];
    int len = snprintf(buffer, sizeof(buffer), "Total items saved in kvs.img: %d\n", saved_items);
    write(1, buffer, len);

    return 0;
}
