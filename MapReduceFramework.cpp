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
};


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
        // lock the mutex because use the input vector
        t_context->client->map(pair.first, pair.second, (void *) t_context);

    }
    pthread_mutex_lock(t_context->mutex);
    std::sort(t_context->intermediate_vec->begin(),
              t_context->intermediate_vec->end(), comparePairs);
    pthread_mutex_unlock(t_context->mutex);
    return nullptr;
}


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
        threadContexts[i] = {&client,
                             &inputVec,
                             &intermediateVectors[i],
                             &outputVec,
                             &mutex,
                             &atomicCounter,
                             multiThreadLevel};
        if (pthread_create(&threads[i], NULL, map_phase,
                           (void *) &threadContexts[i])) {
            std::cerr << "Error creating thread" << std::endl;
            exit(1);
        }
    }

    // Wait for the threads to finish and collect their intermediate results
    for (int i = 0; i < multiThreadLevel; i++) {
        pthread_join(threads[i], NULL);
    }

    // **************** done map phase ****************

//        Free resources
    pthread_mutex_destroy(&mutex);
}


void emit2(K2 *key, V2 *value, void *context) {
    ThreadContext *t_context = (ThreadContext *) context;
    IntermediatePair *pair = new IntermediatePair(key, value);
    pthread_mutex_lock(t_context->mutex);
    t_context->intermediate_vec->push_back(*pair);
    pthread_mutex_unlock(t_context->mutex);
}

