/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

#ifndef SYNCLOCK_H
#define SYNCLOCK_H

#include <pthread.h>

/*
*	Syncronization lock that lets only one thread acquire it.
*/

namespace TPCE
{

class CSyncLock
{
    pthread_mutex_t mutex;
public:
    CSyncLock() {
	pthread_mutex_init(&mutex, NULL);
    }
    ~CSyncLock() {
	pthread_mutex_destroy(&mutex);
    }

	// Acquire lock or block until it is available
	void ClaimLock() {
	    pthread_mutex_lock(&mutex);
	}

	// Release lock so that it can be acquired again
	void ReleaseLock() {
	    pthread_mutex_unlock(&mutex);
	}
};

}	// namespace TPCE

#endif	// #ifndef SYNCLOCK_H
