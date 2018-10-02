# MapReduce-Multi-threaded-Programming
implementation of MapReduce framework. MapReduce is used to parallelise tasks of a specific structure.

Design
The implementation of this design can be split into two parts:
1) Implementing the functions map and reduce.
This will be different for every task. We call this part the client.

2) Implementing everything else – the partition into phases, distribution of work between
threads, synchronisation etc.
This will be identical for different tasks. We call this part the framework.
Using this split, we can code the framework once and then for every new task, we can just code
the significantly smaller and simpler client.


Since part of the task is sorting, every element must have a key that allows us to compare elements
and sort them. For this reason each element is given as a pair (key,value).
We have three types of elements, each having its own key type and value type:
1) Input elements – we denote their key type k1 and value type v1.
2) Intermediary elements – we denote their key type k2 and value type v2.
3) Output elements – we denote their key type k3 and value type v3.

The framework interface consists of three functions:
1) runMapReduceFramework – This function starts runs the entire MapReduce algorithm.
2) emit2 – This function produces a (K2*,V2*) pair.
3) emit3 – This function produces a (K3*,V3*) pair.
