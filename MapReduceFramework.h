#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

/*
    Description: JobHandle is a type definition used to represent a handle or
    identifier for a MapReduce job. It is typically an opaque pointer that allows
    you to refer to a specific MapReduce job instance.
*/
typedef void *JobHandle;

/*
    Description: stage_t is an enumeration that represents the different stages of a
    MapReduce job. It defines four possible stages: UNDEFINED_STAGE, MAP_STAGE,
    SHUFFLE_STAGE, and REDUCE_STAGE, with corresponding integer values.
*/
enum stage_t {
    UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3
};

/*
    Description: JobState is a structure that represents the current state of a
    MapReduce job. It contains two fields: stage, which represents the current
    stage of the job, and percentage, which indicates the progress of the job as a
    floating-point value between 0.0 and 1.0.
*/
typedef struct {
    stage_t stage;
    float percentage;
} JobState;


/*
    Description: emit2 is a function that is typically called within the Map
    function. It is used to emit intermediate key-value pairs during the Map
    phase of the MapReduce job. The function takes a pointer to the key (K2*),
    a pointer to the value (V2*), and a context parameter. The emitted key-value
    pairs are typically collected and processed by the framework.
*/
void emit2(K2 *key, V2 *value, void *context);

/*
    Description: emit3 is a function that is typically called within the Reduce
    function. It is used to emit final key-value pairs during the Reduce phase of
    the MapReduce job. The function takes a pointer to the key (K3*), a pointer to
    the value (V3*), and a context parameter. The emitted key-value pairs are
    typically collected and stored as the final output of the MapReduce job.
*/
void emit3(K3 *key, V3 *value, void *context);

/*
    Description: startMapReduceJob is a function that starts the execution of a
    MapReduce job. It takes several parameters, including a reference to the
    MapReduceClient, the input data vector (inputVec), the output data vector
    (outputVec), and the desired level of multi-threading (multiThreadLevel).
    The function returns a JobHandle that can be used to interact with the running job.
*/
JobHandle startMapReduceJob(const MapReduceClient &client,
                            const InputVec &inputVec, OutputVec &outputVec,
                            int multiThreadLevel);

/*
    Description: waitForJob is a function that blocks the execution until the
    specified MapReduce job (job) completes. It is used to synchronize the main
    program with the completion of the MapReduce job.
*/
void waitForJob(JobHandle job);

/*
    Description: getJobState is a function that retrieves the current state of the
    specified MapReduce job (job) and stores it in the provided JobState structure
    (state). The function allows you to monitor the progress or status of the job
    during its execution.
*/
void getJobState(JobHandle job, JobState *state);

/*
    Description: closeJobHandle is a function used to release system resources
    associated with the specified MapReduce job handle (job). It is called when
    you are done with the job and want to clean up any allocated resources.
*/
void closeJobHandle(JobHandle job);


#endif //MAPREDUCEFRAMEWORK_H
