#include "Barrier.h"
#include <pthread.h>
#include <cstdio>

#define MT_LEVEL 5

struct ThreadContext {
	int threadID;
	Barrier* barrier;
};


void* foo(void* arg)
{

	ThreadContext* tc = (ThreadContext*) arg;
    int temp = tc->threadID;
	printf("Before barriers: %d\n", tc->threadID);
    fflush(stdout);

	tc->barrier->barrier();

	printf("Between barriers: %d\n", tc->threadID);
    fflush(stdout);

	tc->barrier->barrier();

	printf("After barriers: %d\n", tc->threadID);
    fflush(stdout);

	return 0;
}


int main(int argc, char** argv)
{
    printf("start main\n");
    printf("create thread array\n");
	pthread_t threads[MT_LEVEL];
    printf("create contexts array\n");
	ThreadContext contexts[MT_LEVEL];
    printf("create barrier array\n");
	Barrier barrier(MT_LEVEL);
    printf("start first loop:\n");
	for (int i = 0; i < MT_LEVEL; ++i) {
		contexts[i] = {i, &barrier};
	}
    printf("start second loop:\n");

	for (int i = 0; i < MT_LEVEL; ++i) {
		printf("create i=%d\n",i);
        fflush(stdout);
        pthread_create(threads + i, NULL, foo, contexts + i);
	}
    printf("start third loop:\n");
	for (int i = 0; i < MT_LEVEL; ++i) {
		pthread_join(threads[i], NULL);
	}
    printf("Donequit:\n");
	return 0;
}
