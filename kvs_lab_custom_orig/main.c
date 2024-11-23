#include "kvs.h"

node_t* create_node(int level, const char* key, const char* value) {
    node_t* node = (node_t*)malloc(sizeof(node_t));
    custom_strcpy(node->key, key);
    node->value = custom_strdup(value);
    node->forward = (node_t**)malloc((level + 1) * sizeof(node_t*));

    for (int i = 0; i <= level; i++) {
        node->forward[i] = NULL;
    }
    return node;
}

int random_level() {
    int level = 0;
    while ((rand() % 100) < (P * 100) && level < MAX_LEVEL - 1) {
        level++;
    }
    return level;
}

size_t custom_strlen(const char* str) {
    size_t len = 0;
    while (str[len] != '\0') {
        len++;
    }
    return len;
}

void custom_strcpy(char* dest, const char* src) {
    while ((*dest++ = *src++) != '\0');
}

char* custom_strdup(const char* str) {
    size_t len = custom_strlen(str);
    char* new_str = (char*)malloc(len + 1);
    if (new_str) {
        custom_strcpy(new_str, str);
    }
    return new_str;
}

int custom_strcmp(const char* s1, const char* s2) {
    while (*s1 && (*s1 == *s2)) {
        s1++;
        s2++;
    }
    return *(unsigned char*)s1 - *(unsigned char*)s2;
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

    char* line_buffer = NULL; // 라인을 동적으로 저장할 버퍼
    size_t buffer_size = 0;   // 현재 버퍼 크기
    size_t line_length = 0;   // 현재 라인의 길이
    ssize_t bytes_read;       // 읽은 바이트 수
    char c;                   // 한 글자를 읽을 임시 변수

    char operation[4];  // 'set'만 사용
    char key[100];
    char value[5000];   // value의 크기를 5000으로 증가
    int total_set_operations = 0;

    // 한 글자씩 읽어서 라인을 구성
    while ((bytes_read = read(fd, &c, 1)) > 0) {
        // 동적으로 버퍼 크기를 늘림
        if (line_length + 1 >= buffer_size) {
            buffer_size += 1024; // 버퍼 크기 증가
            char* new_buffer = realloc(line_buffer, buffer_size);
            if (!new_buffer) {
                write(2, "Failed to allocate memory for line buffer\n", 43);
                free(line_buffer);
                kvs_close(kvs);
                close(fd);
                return -1;
            }
            line_buffer = new_buffer;
        }

        // 라인 버퍼에 글자 추가
        if (c != '\n') {
            line_buffer[line_length++] = c;
        } else {
            // 라인 종료 처리
            line_buffer[line_length] = '\0'; // 널 문자 추가

            // 라인 파싱
            size_t idx = 0;
            size_t op_idx = 0;

            // operation 파싱
            while (line_buffer[idx] != ',' && line_buffer[idx] != '\0') {
                if (op_idx < 3) {
                    operation[op_idx++] = line_buffer[idx];
                }
                idx++;
            }
            operation[op_idx] = '\0';
            if (line_buffer[idx] == ',') idx++; // ',' 스킵

            // key 파싱
            size_t key_idx = 0;
            while (line_buffer[idx] != ',' && line_buffer[idx] != '\0') {
                if (key_idx < 99) {
                    key[key_idx++] = line_buffer[idx];
                }
                idx++;
            }
            key[key_idx] = '\0';
            if (line_buffer[idx] == ',') idx++; // ',' 스킵

            // value 파싱
            size_t value_idx = 0;
            while (line_buffer[idx] != '\0') {
                if (value_idx < 4999) {
                    value[value_idx++] = line_buffer[idx];
                }
                idx++;
            }
            value[value_idx] = '\0';

            // 'set' 연산 처리
            if (operation[0] == 's' && operation[1] == 'e' && operation[2] == 't') {
                put(kvs, key, value);
                total_set_operations++;
            }

            // 라인 초기화
            line_length = 0;
        }
    }
    if (c != '\0') {
        // 라인 종료 처리
        line_buffer[line_length] = '\0'; // 널 문자 추가

        // 라인 파싱
        size_t idx = 0;
        size_t op_idx = 0;

        // operation 파싱
        while (line_buffer[idx] != ',' && line_buffer[idx] != '\0') {
            if (op_idx < 3) {
                operation[op_idx++] = line_buffer[idx];
            }
            idx++;
        }
        operation[op_idx] = '\0';
        if (line_buffer[idx] == ',') idx++; // ',' 스킵

        // key 파싱
        size_t key_idx = 0;
        while (line_buffer[idx] != ',' && line_buffer[idx] != '\0') {
            if (key_idx < 99) {
                key[key_idx++] = line_buffer[idx];
            }
            idx++;
        }
        key[key_idx] = '\0';
        if (line_buffer[idx] == ',') idx++; // ',' 스킵

        // value 파싱
        size_t value_idx = 0;
        while (line_buffer[idx] != '\0') {
            if (value_idx < 4999) {
                value[value_idx++] = line_buffer[idx];
            }
            idx++;
        }
        value[value_idx] = '\0';

        // 'set' 연산 처리
        if (operation[0] == 's' && operation[1] == 'e' && operation[2] == 't') {
            put(kvs, key, value);
            total_set_operations++;
        }

        // 라인 초기화
        line_length = 0;
    }

    free(line_buffer);
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

