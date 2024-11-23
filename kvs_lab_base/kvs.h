#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define MAX_LEVEL 16 // skip list의 최대 레벨
#define P 0.5        // 레벨 증가 확률

struct node {
    char key[100];
    char* value;
    struct node** forward; // 각 레벨의 다음 노드를 가리키는 포인터 배열
};
typedef struct node node_t;

struct kvs {
    node_t* header; // skip list의 헤더 노드
    int level;      // 현재 skip list의 최대 레벨
    int items;      // 요소의 개수
};
typedef struct kvs kvs_t;

// 함수 선언
kvs_t* kvs_open(const char* filename); // 인자로 파일 이름을 받도록 변경
int kvs_close(kvs_t* kvs);
int put(kvs_t* kvs, const char* key, const char* value);
char* get(kvs_t* kvs, const char* key);

// 기타 함수 선언
node_t* create_node(int level, const char* key, const char* value);
int random_level();
int do_snapshot(kvs_t* kvs);
int do_recovery(kvs_t* kvs, const char* filename); // 인자로 파일 이름을 받도록 변경
