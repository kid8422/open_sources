#include "kvs.h"

int do_recovery(kvs_t* kvs, const char* filename) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        write(2, "Failed to open kvs.img for recovery\n", 36);
        return -1; // 복구 실패
    }

    int recovered_items = 0;

    while (1) {
        uint32_t key_len;
        ssize_t bytes_read = read(fd, &key_len, sizeof(uint32_t));
        if (bytes_read == 0) {
            break; // 파일 끝
        }
        if (bytes_read != sizeof(uint32_t)) {
            write(2, "Failed to read key length\n", 26);
            close(fd);
            return -1;
        }

        if (key_len >= 100) {
            write(2, "Key length too long\n", 20);
            close(fd);
            return -1;
        }

        char key[100];
        read(fd, key, key_len);
        key[key_len] = '\0';

        uint32_t value_len;
        read(fd, &value_len, sizeof(uint32_t));

        if (value_len > 5000) {
            write(2, "Value length too long\n", 22);
            close(fd);
            return -1;
        }

        char* value = (char*)malloc(value_len + 1);
        if (!value) {
            write(2, "Failed to allocate memory for value\n", 36);
            close(fd);
            return -1;
        }
        read(fd, value, value_len);
        value[value_len] = '\0';

        put(kvs, key, value);

        free(value);
        recovered_items++;
    }

    close(fd);

    // 복구된 아이템 수 출력
    char buffer[100];
    int len = snprintf(buffer, sizeof(buffer), "Total items recovered from %s: %d\n", filename, recovered_items);
    write(1, buffer, len);

    return 0;
}
