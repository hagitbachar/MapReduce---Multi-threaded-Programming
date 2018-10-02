//
// Created by yael.chaimov on 5/31/18.
//

#include "Barrier.h"
#include <cstdio>
#include <atomic>
#include "MapReduceFramework.h"

#include <iostream>
#include <semaphore.h>
#include <algorithm>

#define INIT_ATOMIC_COUNTER 0
#define INIT_SEMAPHORE_COUNTER 0
/**
 * context of each thread
 */
struct Context{
    int threadID;
    Barrier* barrier;
    int multiThreadLevel;
    const InputVec* inputVec;
    OutputVec* outputVec;
    std::vector<IntermediateVec>* vector_of_intermediateVectors;
    std::vector<IntermediateVec>* vector_of_shuffled_intermediateVec;
    const MapReduceClient* client;
    std::atomic<int>* atomic_counter;
    sem_t *semaphore;
    pthread_mutex_t *general_mutex;
    pthread_mutex_t *emit3_mutex;
    bool * shuffle_done;
};

/**
 * release all memory
 * @param context - pointer to thread context
 */
void delete_memory(Context* context)
{
    pthread_mutex_destroy(context->general_mutex);
    pthread_mutex_destroy(context->emit3_mutex);
    sem_destroy(context->semaphore);
    delete[] context->vector_of_intermediateVectors;
    delete[] context->vector_of_shuffled_intermediateVec;
}
/**
 * insert pair to intermediate vector
 * @param key - the key of the pair
 * @param value - the value of the pair
 * @param context - pointer to thread context
 */
void emit2 (K2* key, V2* value, void* context)
{
    IntermediatePair intermediatePair(key, value);
    auto *tc = (Context *)context;
    try {
        tc->vector_of_intermediateVectors->at(tc->threadID).push_back(intermediatePair);
    }
    catch (const std::bad_alloc &)
    {
        std::cerr<<"system error - alloc failed"<<std::endl;
        delete_memory(tc);
        exit(1);
    }
}
/**
 * insert pair to output vector
 * @param key - the key of the pair
 * @param value - the value of the pair
 * @param context - pointer to thread context
 */
void emit3 (K3* key, V3* value, void* context)
{

    OutputPair outputPair(key, value);
    auto *tc = (Context *)context;

    if(pthread_mutex_lock(tc->emit3_mutex))
    {
        std::cerr<<"system error - pthread_mutex_lock failed"<<std::endl;
        delete_memory(tc);
        exit(1);
    }
    try {
        tc->outputVec->push_back(outputPair);

    }
    catch (const std::bad_alloc &)
    {
        std::cerr<<"system error - alloc failed"<<std::endl;
        delete_memory(tc);
        exit(1);
    }
    if(pthread_mutex_unlock(tc->emit3_mutex) !=0)
    {
        std::cerr<<"system error - pthread_mutex_unlock failed"<<std::endl;
        delete_memory(tc);
        exit(1);
    }
}

/**
 * the function that compare between the keys
 * @param pair1 - the first pair
 * @param pair2 - the second pair
 * @return true iff the first smaller than the second
 */
bool order(const IntermediatePair& pair1, const IntermediatePair& pair2)
{
    return (*(pair1.first) < *(pair2.first));
}

/**
 * create new sequences of (k2,v2) where in each sequence all keys are
 * identical and all elements with a given key are in a single sequence.
 * @param context - pointer to thread context
 */
void shuffle(Context* context)
{

    IntermediateVec *first_intermediateVec;
    K2* maxK2;
    int size = context->multiThreadLevel;
    while(not context->vector_of_intermediateVectors->empty())
    {
        //find maximum key
        IntermediateVec to_reduce;
        first_intermediateVec = &context->vector_of_intermediateVectors->at(0);
        while (first_intermediateVec->empty())
        {
            context->vector_of_intermediateVectors->erase(context->vector_of_intermediateVectors->begin());
            size--;
            first_intermediateVec = &context->vector_of_intermediateVectors->at(0);
        }
        maxK2 = first_intermediateVec->back().first;
        for( int i = 0; i< size; i++ ){

            IntermediateVec* ith_intermediateVec = &context->vector_of_intermediateVectors->at(i);
            if (ith_intermediateVec->empty())
            {
                context->vector_of_intermediateVectors->erase(context->vector_of_intermediateVectors->begin()+i);
                i--;
                size--;
            }
            else if(*maxK2 < *ith_intermediateVec->back().first)
            {
                maxK2 = ith_intermediateVec->back().first;
            }
        }
        //insert maximum to to_reduce
        for( int i = 0; i< size;i++ )
        {

            IntermediateVec* ith_intermediateVec =& (context->vector_of_intermediateVectors->at(i));
            while( !ith_intermediateVec->empty() && !(*ith_intermediateVec->back().first < *maxK2))
            {
                IntermediatePair pair = (ith_intermediateVec->back());
                if (pair.first == NULL)
                {
                    ith_intermediateVec->pop_back();
                    continue;
                }
                try{

                    to_reduce.push_back(pair);
                }
                catch (const std::bad_alloc &)
                {
                    std::cerr<<"system error - alloc failed"<<std::endl;
                    delete_memory(context);
                    exit(1);
                }
                ith_intermediateVec->pop_back();
            }
            if (ith_intermediateVec->empty())
            {
                context->vector_of_intermediateVectors->erase(context->vector_of_intermediateVectors->begin()+i);
                i--;
                size--;
            }
        }
        if(pthread_mutex_lock(context->general_mutex))
        {
            std::cerr<<"system error - pthread_mutex_lock failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        try{
            context->vector_of_shuffled_intermediateVec->push_back(to_reduce);

        }
        catch (const std::bad_alloc &)
        {
            std::cerr<<"system error - alloc failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        if(pthread_mutex_unlock(context->general_mutex))
        {
            std::cerr<<"system error - pthread_mutex_unlock failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        if(sem_post(context->semaphore))
        {
            std::cerr<<"system error - sem_post failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
    }
    *(context->shuffle_done) = true;
}
/**
 * increase the semaphore to release all stuck threads
 * @param context - pointer to thread context
 */
void sem_post_for_all_threads(Context * context)
{
    for(int i = 0; i < context->multiThreadLevel - 1; i++)
    {
        if (sem_post(context->semaphore))
        {
            std::cerr << "system error - sem_post failed" << std::endl;
            delete_memory(context);

            exit(1);

        }
    }
}
/**
 * the function that each thread run
 * @param arg - pointer to thread context as void *
 * @return nullptr on success
 */
void* logic(void* arg)
{
    //map
    Context* context = (Context*)arg;
    while (true)
    {
        int old_value = (*(context->atomic_counter))++;
        if(old_value >= (int)context->inputVec->size())
        {
            break;
        }
        auto * K1 = context->inputVec->at(old_value).first;
        auto * V1 = context->inputVec->at(old_value).second;
        context->client->map(K1, V1, arg);
    }

    //sort
    std::sort(context->vector_of_intermediateVectors->at(context->threadID).begin(),
                   context->vector_of_intermediateVectors->at(context->threadID).end(), order);

    //barrier
    context->barrier->barrier();



    //shuffle
    //std::vector<IntermediateVec> * vector_of_shuffled_intermediateVec = new std::vector<IntermediateVec>();
    if(context->threadID == 0)
    {
        shuffle(context);
    }

    //reduce
    while(true)
    {
        if(pthread_mutex_lock(context->general_mutex))
        {
            std::cerr<<"system error - pthread_mutex_lock failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        if(context->shuffle_done && context->vector_of_shuffled_intermediateVec->empty())
        {
            if(pthread_mutex_unlock(context->general_mutex))
            {
                std::cerr<<"system error - pthread_mutex_unlock failed"<<std::endl;
                delete_memory(context);
                exit(1);
            }
            break;
        }
        if(pthread_mutex_unlock(context->general_mutex))
        {
            std::cerr<<"system error - pthread_mutex_unlock failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        if(sem_wait(context->semaphore))
        {
            std::cerr<<"system error - sem_wait failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        if(pthread_mutex_lock(context->general_mutex))
        {
            std::cerr<<"system error - pthread_mutex_lock failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }
        if(context->shuffle_done && context->vector_of_shuffled_intermediateVec->empty())
        {
            if(pthread_mutex_unlock(context->general_mutex))
            {
                std::cerr<<"system error - pthread_mutex_unlock failed"<<std::endl;
                delete_memory(context);
                exit(1);
            }
            break;
        }
        IntermediateVec to_reduce =(context->vector_of_shuffled_intermediateVec->back());
        context->vector_of_shuffled_intermediateVec->pop_back();
        if(pthread_mutex_unlock(context->general_mutex))
        {
            std::cerr<<"system error - pthread_mutex_unlock failed"<<std::endl;
            delete_memory(context);
            exit(1);
        }

        context->client->reduce(&to_reduce, arg);


    }
    //increase the semaphore to release all stuck threads
    sem_post_for_all_threads(context);


    return nullptr;
}

/**
 * the function that create the threads and start the map reduce process
 * according to map reduce algorithm
 * @param client - the implementation of the map and reduce functions
 * @param inputVec - the input vector
 * @param outputVec - the output vector
 * @param multiThreadLevel - the number of threads that need to be create for the process
 */
void runMapReduceFramework(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec,
                           int multiThreadLevel)
{
    bool shuffle_done = false;
    sem_t semaphore ;
    sem_init(&semaphore, 0, INIT_SEMAPHORE_COUNTER);
    pthread_mutex_t general_mutex;
    pthread_mutex_init(&general_mutex, NULL);
    pthread_mutex_t emit3_mutex;
    pthread_mutex_init(&emit3_mutex, NULL);
    pthread_t * threads = new (std::nothrow) pthread_t[multiThreadLevel];
    Context *contexts = new (std::nothrow) Context[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    std::atomic<int> atomic_counter(INIT_ATOMIC_COUNTER);
    std::vector<IntermediateVec> vector_of_intermediateVectors;
    std::vector<IntermediateVec> vector_of_shuffled_intermediateVec;
    for(int i = 0; i < multiThreadLevel; i++)
    {
        IntermediateVec temp;
        vector_of_intermediateVectors.push_back(temp);
    }
    //create multiThreadLevel context for each thread
    for (int i = 0; i < multiThreadLevel; ++i) {
        contexts[i] = {i, &barrier, multiThreadLevel, &inputVec, &outputVec, &vector_of_intermediateVectors,
       &vector_of_shuffled_intermediateVec, &client, &atomic_counter,&semaphore ,&general_mutex, &emit3_mutex,
                                                                                                &shuffle_done};
    }
    //create multiThreadLevel - 1 threads because thread 0 is already exist
    for (int i = 0; i < multiThreadLevel - 1; ++i) {
        pthread_create(threads + i, NULL, logic, contexts + i + 1);
    }
    //call the thread 0 function with context 0
    logic(contexts + 0);
    for (int i = 0; i < multiThreadLevel - 1; ++i) {
        pthread_join(threads[i], NULL);
    }
    pthread_mutex_destroy(&general_mutex);
    pthread_mutex_destroy(&emit3_mutex);
    sem_destroy(&semaphore);
    delete [] threads;
    delete [] contexts;
}