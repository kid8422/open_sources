#include "kvs.h"

node_t* create_node(int level, const char* key, const char* value) {
    node_t* node = (node_t*)malloc(sizeof(node_t));
    if (!node) return NULL; // 메모리 할당 실패 처리

    custom_strcpy(node->key, key);
    node->value = custom_strdup(value);
    if (!node->value) {
        free(node);
        return NULL; // 메모리 할당 실패 처리
    }

    node->forward = (node_t**)malloc((level + 1) * sizeof(node_t*));
    if (!node->forward) {
        free(node->value);
        free(node);
        return NULL; // 메모리 할당 실패 처리
    }

    int i = 0;
    int max = level + 1;
    for (; i + 3 < max; i += 4) {
        node->forward[i] = NULL;
        node->forward[i + 1] = NULL;
        node->forward[i + 2] = NULL;
        node->forward[i + 3] = NULL;
    }
    for (; i < max; i++) {
        node->forward[i] = NULL;
    }
    return node;
}

int random_level() {
    int level = 0;
    while ((rand() & 0xFFFF) < (P * 0xFFFF) && level < MAX_LEVEL - 1) {
        level++;
    }
    return level;
}

size_t custom_strlen(const char* str) {
    const char* s = str;

    // 4바이트씩 검사하는 루프 언롤링
    while (1) {
        if (!s[0]) return s - str;
        if (!s[1]) return s - str + 1;
        if (!s[2]) return s - str + 2;
        if (!s[3]) return s - str + 3;
        s += 4;
    }
}

void custom_strcpy(char* dest, const char* src) {
    while ((*dest++ = *src++));
}

char* custom_strdup(const char* str) {
    size_t len = custom_strlen(str);
    char* new_str = (char*)malloc(len + 1);
    if (!new_str) return NULL; // 메모리 할당 실패 처리
    custom_strcpy(new_str, str);
    return new_str;
}

int custom_strcmp(const char* s1, const char* s2) {
    while (1) {
        if (s1[0] != s2[0] || !s1[0]) return (unsigned char)s1[0] - (unsigned char)s2[0];
        if (s1[1] != s2[1] || !s1[1]) return (unsigned char)s1[1] - (unsigned char)s2[1];
        if (s1[2] != s2[2] || !s1[2]) return (unsigned char)s1[2] - (unsigned char)s2[2];
        if (s1[3] != s2[3] || !s1[3]) return (unsigned char)s1[3] - (unsigned char)s2[3];
        s1 += 4;
        s2 += 4;
    }
}

char* custom_strchr(const char* s, int c) {
    char ch = (char)c;
    while (1) {
        if (s[0] == ch) return (char*)&s[0];
        if (!s[0]) return NULL;
        if (s[1] == ch) return (char*)&s[1];
        if (!s[1]) return NULL;
        if (s[2] == ch) return (char*)&s[2];
        if (!s[2]) return NULL;
        if (s[3] == ch) return (char*)&s[3];
        if (!s[3]) return NULL;
        s += 4;
    }
}

int main() {
    // 시간 측정 변수
    clock_t start_time, end_time;
    double recovery_time, workload_time, snapshot_time;

    char buffer[100];
    int len;

    // 1. 처음 open에는 인자를 안 넣어주고, do_recovery를 실행하지 않음
    start_time = clock();
    kvs_t* kvs = kvs_open(NULL);
    end_time = clock();

    recovery_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    len = snprintf(buffer, sizeof(buffer), "첫 번째 kvs_open 실행 시간: %f 초\n", recovery_time);
    write(1, buffer, len);

    if (!kvs) {
        write(1, "Failed to open kvs\n", 19);
        return -1;
    }

    // KVS 상태 출력 (첫 번째 open 후)
    len = snprintf(buffer, sizeof(buffer), "Initial kvs items: %d\n", kvs->items);
    write(1, buffer, len);

    // 2. cluster004.trc 파일을 불러와서 워크로드 실행
    int fd = open("cluster004.trc", O_RDONLY);
    if (fd < 0) {
        write(1, "Failed to open cluster004.trc\n", 30);
        kvs_close(kvs);
        return -1;
    }

    // 워크로드 실행 시간 측정 시작
    start_time = clock();

    // 파일 크기 얻기
    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    // 파일 전체를 메모리에 로드
    char* file_buffer = (char*)malloc(file_size + 1);
    if (!file_buffer) {
        write(1, "Failed to allocate memory for file buffer\n", 42);
        kvs_close(kvs);
        close(fd);
        return -1;
    }

    ssize_t bytes_read = read(fd, file_buffer, file_size);
    if (bytes_read != file_size) {
        write(1, "Failed to read the entire file\n", 31);
        free(file_buffer);
        kvs_close(kvs);
        close(fd);
        return -1;
    }
    file_buffer[file_size] = '\0'; // 문자열 종료

    close(fd);

    // 라인 파싱 및 처리
    char* line = file_buffer;
    char* line_end;
    int total_set_operations = 0;

    while ((line_end = custom_strchr(line, '\n')) != NULL) {
        *line_end = '\0';

        // 라인 파싱
        char* op = line;
        char* key = custom_strchr(op, ',');
        if (!key) break;
        *key++ = '\0';

        char* value = custom_strchr(key, ',');
        if (!value) break;
        *value++ = '\0';

        // 'set' 연산 처리
        if (op[0] == 's' && op[1] == 'e' && op[2] == 't') {
            put(kvs, key, value);
            total_set_operations++;
        }

        line = line_end + 1;
    }

    // 마지막 라인 처리
    if (*line != '\0') {
        // 라인 파싱
        char* op = line;
        char* key = custom_strchr(op, ',');
        if (key) {
            *key++ = '\0';
            char* value = custom_strchr(key, ',');
            if (value) {
                *value++ = '\0';

                // 'set' 연산 처리
                if (op[0] == 's' && op[1] == 'e' && op[2] == 't') {
                    put(kvs, key, value);
                    total_set_operations++;
                }
            }
        }
    }

    free(file_buffer);

    close(fd);

    // 워크로드 실행 시간 측정 종료
    end_time = clock();
    workload_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    len = snprintf(buffer, sizeof(buffer), "워크로드 실행 시간: %f 초\n", workload_time);
    write(1, buffer, len);

    // KVS 상태 출력 (워크로드 실행 후)
    len = snprintf(buffer, sizeof(buffer), "Total set operations: %d\n", total_set_operations);
    write(1, buffer, len);
    len = snprintf(buffer, sizeof(buffer), "Total unique keys stored: %d\n", kvs->items);
    write(1, buffer, len);

    // 3. do_snapshot으로 저장
    start_time = clock();
    do_snapshot(kvs);
    end_time = clock();
    snapshot_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    len = snprintf(buffer, sizeof(buffer), "do_snapshot 실행 시간: %f 초\n", snapshot_time);
    write(1, buffer, len);

    // KVS 닫기
    kvs_close(kvs);

    // 4. open에 kvs.img를 인자로 주고 kvs를 생성하면서 불러옴
    start_time = clock();
    kvs_t* kvs_loaded = kvs_open("kvs.img");
    end_time = clock();
    recovery_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    len = snprintf(buffer, sizeof(buffer), "두 번째 kvs_open (복구) 실행 시간: %f 초\n", recovery_time);
    write(1, buffer, len);

    if (!kvs_loaded) {
        write(1, "Failed to open kvs with kvs.img\n", 32);
        return -1;
    }

    // KVS 상태 출력 (두 번째 open 후)
    len = snprintf(buffer, sizeof(buffer), "Loaded kvs items: %d\n", kvs_loaded->items);
    write(1, buffer, len);

    // KVS 닫기
    kvs_close(kvs_loaded);

    return 0;
}

