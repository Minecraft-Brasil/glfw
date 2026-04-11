//
// Created by maks on 11.04.2026.
//

#include <string.h>
#include <stdatomic.h>

#include "android_input_queue.h"

static bool queue_init_single(queue_t* queue) {
    queue->events_total = 0;
    if(pthread_mutex_init(&queue->wait_mutex, NULL) != 0) return false;
    if(pthread_cond_init(&queue->wait_cond, NULL) != 0) goto fail;
    return true;
    fail:
    pthread_mutex_destroy(&queue->wait_mutex);
    return false;
}

static void queue_destroy_single(queue_t* queue) {
    pthread_mutex_destroy(&queue->wait_mutex);
    pthread_cond_destroy(&queue->wait_cond);
}

bool _input_queue_init(queue_top_t* top) {
    top->index = 0;
    int initialized;
    bool ok = true;
    for(initialized = 0; initialized < 2; initialized++) {
        if(!(ok = queue_init_single(&top->queues[initialized]))) break;
    }
    if(ok) return true;
    for(int i = 0; i < initialized + 1; i++) {
        queue_destroy_single(&top->queues[i]);
    }
    return false;
}

void _input_queue_destroy(queue_top_t* top) {
    for(int i = 0; i < 2; i++) {
        queue_destroy_single(&top->queues[i]);
    }
}

void _input_queue_push(queue_top_t* top, input_event_t* event) {
    queue_t *current = &top->queues[atomic_load(&top->index)];
    uint16_t ev_idx = atomic_fetch_add(&current->events_total, 1);
    input_event_t *put_event = &current->events[ev_idx];
    if(ev_idx == 0) {
        pthread_mutex_lock(&current->wait_mutex);
        pthread_cond_broadcast(&current->wait_cond);
        pthread_mutex_unlock(&current->wait_mutex);
    }
    memcpy(put_event, event, sizeof(input_event_t));
}

void _input_queue_dequeue(queue_top_t* top, dequeue_callback_t cb) {
    // Atomically switch queue 0 to 1 (or 1 to 0) by flipping the bit
    queue_t *current = &top->queues[atomic_fetch_xor(&top->index, 0b1)];
    for(uint16_t i = 0; i < current->events_total; i++) {
        cb(&current->events[i]);
    }
    current->events_total = 0;
}

void _input_queue_wait(queue_top_t* top, dequeue_callback_t cb, struct timespec* timeout) {
    queue_t *current = &top->queues[top->index];
    if(current->events_total == 0) {
        pthread_mutex_lock(&current->wait_mutex);
        if(timeout->tv_sec == 0 && timeout->tv_nsec == 0)
            pthread_cond_wait(&current->wait_cond, &current->wait_mutex);
        else
            pthread_cond_timedwait(&current->wait_cond, &current->wait_mutex, timeout);
        pthread_mutex_unlock(&current->wait_mutex);
    }
    _input_queue_dequeue(top, cb);
}

