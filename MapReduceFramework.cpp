#include "MapReduceFramework.h"
#include <pthread.h>
#include <cstdio>
#include <atomic>
#include <iostream>
#include <algorithm>
#include <vector>  //std::vector
#include <utility> //std::pair

typedef struct ThreadContext {
    const MapReduceClient *client;
    const InputVec *input_vec;
    IntermediateVec *intermediate_vec;
    OutputVec *output_vec;
    pthread_mutex_t *mutex;
    std::atomic<int> *atomicCounter;
    int multiThreadLevel;
    JobState *current_state;
    std::vector<IntermediateVec> *intermediate_vecs;
} ThreadContext;

typedef struct ShuffleContext {
    pthread_mutex_t *mutex;
    std::atomic<int> *atomicCounter;
    JobState *current_state;
    std::vector<IntermediateVec> *intermediate_vecs;
} ShuffleContext;

typedef struct WaitContext {
    std::vector<pthread_t> *threads;
    int multiThreadLevel;
} WaitContext;

JobState current_state;

bool comparePairs(const std::pair<K2 *, V2 *> &pair1,
                  const std::pair<K2 *, V2 *> &pair2) {
    return *pair1.first < *pair2.first;
}


void *map_phase(void *context) {
    ThreadContext *t_context = (ThreadContext *) context;
    while (true) {
        int currentIndex = t_context->atomicCounter->fetch_add(1);
        pthread_mutex_lock(t_context->mutex);
        if (currentIndex >= (int) t_context->input_vec->size()) {
            pthread_mutex_unlock(t_context->mutex);
            break;
        }
        pthread_mutex_unlock(t_context->mutex);

        std::pair<K1 *, V1 *> pair = (t_context->input_vec)->at(currentIndex);

        // TODO: CHECK IF NEED TO lock the mutex because use the input vector
        t_context->client->map(pair.first, pair.second, (void *) t_context);

    }
    pthread_mutex_lock(t_context->mutex);
    std::sort(t_context->intermediate_vec->begin(),
              t_context->intermediate_vec->end(), comparePairs);
    pthread_mutex_unlock(t_context->mutex);
    return nullptr;
}


void shuffle_phase(ShuffleContext &context) {

}


void *reduce_phase(void *context) {
    return nullptr;
}


//    ThreadContext *t_context = (ThreadContext *) context;
//
//    // Keep processing intermediate key-value pairs until all pairs have been reduced
//    while (true) {
//        int currentKeyIndex = t_context->atomicCounter->fetch_add(1);
//
//        // Check if all keys have been processed by other threads already
//        if (currentKeyIndex >= (int) t_context->keysVec->size()) {
//            break;
//        }
//
//        K2 *key = t_context->keysVec->at(currentKeyIndex);
//
//        // Collect all intermediate values associated with the current key
//        std::vector<V2 *> valuesVec;
//        for (int i = 0; i < t_context->intermediateVecs->size(); i++) {
//            IntermediateVec *vec = t_context->intermediateVecs->at(i);
//            for (int j = 0; j < vec->size(); j++) {
//                IntermediatePair *pair = &vec->at(j);
//                if (*(pair->first) == *key) {
//                    valuesVec.push_back(pair->second);
//                }
//            }
//        }
//
//        // Reduce the values and emit the final key-value pair
//        V2 *outputValue = t_context->client->reduce(key, valuesVec, t_context);
//    }
//    return nullptr;
//}


/*
 * Implement the startMapReduceJob function in MapReduceFramework.cpp.
 * This function should create the necessary threads and distribute the work among them.
 * Use the atomic variable to split the input values between the threads.
 * Call the map function on each pair and use the emit2 function to update the framework's databases.
 *
    Description: startMapReduceJob is a function that starts the execution of a
    MapReduce job. It takes several parameters, including a reference to the
    MapReduceClient, the input data vector (inputVec), the output data vector
    (outputVec), and the desired level of multi-threading (multiThreadLevel).
    The function returns a JobHandle that can be used to interact with the running job.
*/
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel) {

    // create empty vector for all the threads
    std::vector<pthread_t> threads(multiThreadLevel);

    // This vector is used to store the intermediate results generated by each thread during the Map phase.
    std::vector<IntermediateVec> intermediateVectors(multiThreadLevel);

    // mutex locks that are used to synchronize the access to the shared vectors
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    // init the mutex
    pthread_mutex_init(&mutex, NULL);

    // an array to store all the context for each thread
    ThreadContext threadContexts[multiThreadLevel];

    // atomic counter that are used to keep track of the number of Map and Reduce tasks
    // that have been completed by each thread
    std::atomic<int> atomicCounter(0);


    for (int i = 0; i < multiThreadLevel; ++i) {
        map_thread_contexts[i] = {&client,
                                  &inputVec,
                                  &intermediateVectors[i],
                                  nullptr,
                                  &mutex,
                                  &atomicCounter,
                                  multiThreadLevel,
                                  &current_state,
                                  &intermediateVectors};
        if (pthread_create(&map_threads[i], NULL, map_phase,
                           (void *) &map_thread_contexts[i])) {
            std::cerr << "Error creating thread" << std::endl;
            exit(1);
        }
    }

    // Wait for the threads to finish and collect their intermediate results
    WaitContext curr_wait = {&map_threads, multiThreadLevel};
    waitForJob(&curr_wait);

    // Update the job state to the shuffle phase
    current_state.stage = SHUFFLE_STAGE;
    ShuffleContext shuffle_context = {
            &mutex,
            &atomicCounter,
            &current_state,
            &intermediateVectors};

    // NOW perform the shuffle phase:
    shuffle_phase(shuffle_context);

    // Update the job state to the reduce phase
    current_state.stage = REDUCE_STAGE;

    // create empty vector for all the threads
    std::vector<pthread_t> reduce_threads(multiThreadLevel);

    // an array to store all the context for each thread
    ThreadContext reduce_threads_context[multiThreadLevel];

    for (int i = 0; i < multiThreadLevel; ++i) {
        reduce_threads_context[i] = {&client,
                                     &inputVec,
                                     &intermediateVectors[i],
                                     nullptr,
                                     &mutex,
                                     &atomicCounter,
                                     multiThreadLevel,
                                     &current_state,
                                     &intermediateVectors};
        if (pthread_create(&reduce_threads[i], NULL, reduce_phase,
                           (void *) &reduce_threads_context[i])) {
            std::cerr << "Error creating thread" << std::endl;
            exit(1);
        }
    }

    curr_wait = {&reduce_threads, multiThreadLevel};
    waitForJob(&curr_wait);

    current_state.stage = UNDEFINED_STAGE;
    current_state.percentage = 100;

//        Free resources
    pthread_mutex_destroy(&mutex);
}



//void *reduce_phase(void *context) {
//    ThreadContext *t_context = (ThreadContext *) context;
//
//    // Keep processing intermediate key-value pairs until all pairs have been reduced
//    while (true) {
//        int currentKeyIndex = t_context->atomicCounter->fetch_add(1);
//
//        // Check if all keys have been processed by other threads already
//        if (currentKeyIndex >= (int) t_context->keysVec->size()) {
//            break;
//        }
//
//        K2 *key = t_context->keysVec->at(currentKeyIndex);
//
//        // Collect all intermediate values associated with the current key
//        std::vector<V2 *> valuesVec;
//        for (int i = 0; i < t_context->intermediateVecs->size(); i++) {
//            IntermediateVec *vec = t_context->intermediateVecs->at(i);
//            for (int j = 0; j < vec->size(); j++) {
//                IntermediatePair *pair = &vec->at(j);
//                if (*(pair->first) == *key) {
//                    valuesVec.push_back(pair->second);
//                }
//            }
//        }
//
//        // Reduce the values and emit the final key-value pair
//        V2 *outputValue = t_context->client->reduce(key, valuesVec, t_context);
//    }
//    return nullptr;
//}



void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *t_context = (ThreadContext *) context;
    IntermediatePair *pair = new IntermediatePair(key, value);
    pthread_mutex_lock(t_context->mutex);
    t_context->intermediate_vec->push_back(*pair);
    pthread_mutex_unlock(t_context->mutex);
}


void emit3(K3 *key, V3 *value, void *context) {}


void waitForJob(JobHandle job) {
    WaitContext *curr = (WaitContext *) job;
    for (int i = 0; i < curr->multiThreadLevel; i++) {
        pthread_join((*curr->threads)[i], NULL);
    }
}


void getJobState(JobHandle job, JobState *state) {
    ThreadContext *t_job = (ThreadContext *) job;
    state->stage = t_job->current_state->stage;
    state->percentage = t_job->current_state->percentage;
}


void closeJobHandle(JobHandle job) {
    ThreadContext *t_job = (ThreadContext *) job;
    for (auto vec: *(t_job->intermediate_vecs)) {
        for (auto pair: vec) {
            delete &pair;
        }
    }
}

