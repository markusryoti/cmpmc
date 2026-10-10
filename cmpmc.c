#include <pthread.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NUM_PRODUCERS 5
#define NUM_CONSUMERS 10
#define QUEUE_LENGTH 5
#define NUM_JOBS 100

volatile int num_added = 0;
volatile int num_processed = 0;

pthread_mutex_t mutex;
pthread_cond_t empty, fill;

typedef struct Queue {
    int data[QUEUE_LENGTH];
    int current_add_pos;
    int current_pop_pos;
    int queue_full;
    int count;
} Queue;

typedef struct ThreadParams {
    size_t id;
    Queue *queue;
} ThreadParams;

void put(Queue *queue, int value) {
    queue->data[queue->current_add_pos] = value;
    queue->current_add_pos = (queue->current_add_pos + 1) % QUEUE_LENGTH;
    queue->count++;
}

int get(Queue *queue) {
    int tmp = queue->data[queue->current_pop_pos];
    queue->current_pop_pos = (queue->current_pop_pos + 1) % QUEUE_LENGTH;
    queue->count--;
    return tmp;
}

void *producer(void *args) {
    ThreadParams *params = (ThreadParams *)args;
    Queue *queue = params->queue;

    printf("producer %zu running\n", params->id);

    while (1) {
        pthread_mutex_lock(&mutex);

        if (num_added >= NUM_JOBS) {
            pthread_cond_signal(&empty);
            pthread_mutex_unlock(&mutex);
            break;
        }

        while (queue->count == QUEUE_LENGTH) {
            printf("queue is full, producer=%zu\n", params->id);
            pthread_cond_wait(&empty, &mutex);
        }

        if (num_added >= NUM_JOBS) {
            pthread_cond_signal(&empty);
            pthread_mutex_unlock(&mutex);
            break;
        }

        put(queue, ++num_added);

        printf("producer=%zu added item=%d\n", params->id, num_added);

        pthread_cond_signal(&fill);
        pthread_mutex_unlock(&mutex);

        sleep(1);
    }

    printf("producer %zu exiting\n", params->id);

    return NULL;
}

void *consumer(void *args) {
    ThreadParams *params = (ThreadParams *)args;
    Queue *queue = params->queue;

    printf("consumer %zu running\n", params->id);

    while (1) {
        pthread_mutex_lock(&mutex);

        if (num_processed >= NUM_JOBS) {
            queue->count = -1;
            pthread_cond_signal(&fill);
            pthread_mutex_unlock(&mutex);
            break;
        }

        while (queue->count == 0) {
            printf("queue is empty, consumer=%zu\n", params->id);
            pthread_cond_wait(&fill, &mutex);
        }

        if (num_processed >= NUM_JOBS) {
            queue->count = -1;
            pthread_cond_signal(&fill);
            pthread_mutex_unlock(&mutex);
            break;
        }

        int item = get(queue);

        printf("consumer=%zu processing item=%d\n", params->id, item);

        num_processed++;

        pthread_cond_signal(&empty);
        pthread_mutex_unlock(&mutex);

        sleep(1);
    }

    printf("consumer %zu exiting\n", params->id);

    return NULL;
}

void create_threads(void *fn, pthread_t *threads, Queue *queue,
                    size_t num_threads) {
    for (size_t i = 0; i < num_threads; i++) {
        ThreadParams *mem = malloc(sizeof(ThreadParams));
        mem->id = i;
        mem->queue = queue;

        pthread_create(&threads[i], NULL, fn, (void *)mem);
    }
}

void wait_for_threads(pthread_t *threads, size_t num_threads) {
    for (size_t i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
}

int main() {
    pthread_t producers[NUM_PRODUCERS];
    pthread_t consumers[NUM_CONSUMERS];

    Queue queue;
    queue.current_add_pos = 0;
    queue.current_pop_pos = 0;
    queue.queue_full = 0;
    queue.count = 0;

    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&empty, NULL);
    pthread_cond_init(&fill, NULL);

    printf("mutex and condition variables initialized\n");

    create_threads(consumer, consumers, &queue, NUM_CONSUMERS);
    printf("all consumers started\n");

    create_threads(producer, producers, &queue, NUM_PRODUCERS);
    printf("all producers started\n");

    wait_for_threads(producers, NUM_PRODUCERS);
    printf("all producers exited\n");

    wait_for_threads(consumers, NUM_CONSUMERS);
    printf("all consumers exited\n");

    printf("all threads exited\n");

    return 0;
}
