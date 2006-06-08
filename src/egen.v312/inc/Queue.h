/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	FIFO Queue class template.
*	Used to avoid using STL templates.
*/
#ifndef QUEUE_H
#define QUEUE_H

namespace TPCE
{

// Declare return code constants for different CQueue calls
enum eQueueReturnCode
{
	eEnqueueFailed = -1,
	eDequeueFailed = 0
};

template <typename T>
class CQueue
{
	T*		m_buffer;	//the actual storage for queue elements
	int		m_iSize;	//available size of the queue
	int		m_iCurPlaceIndex;	//index where to place the next element
	int		m_iCurPickIndex;	//index where to remove the next element
	int		m_iCurSize;			//current number of elements in the queue
	CSyncLock	m_CurLock;	//lock to protect modifications to the buffer

public:
	CQueue(int iSize)
		: m_iSize(iSize), m_iCurPlaceIndex(0), m_iCurPickIndex(0), m_iCurSize(0)
	{
		if ( (m_buffer = new T[m_iSize])==NULL )
			throw new CMemoryErr((char*)"CQueue::CQueue");
	}

	~CQueue()
	{
		if (m_buffer!=NULL)
			delete[] m_buffer;
	}

	// Put an element onto the queue.
	// Return:
	//    - new queue size if insert succeeded
	//    - (-1) if the queue was full and the element could not be inserted
	int enqueue(T &elem)
	{
		int iRet;

		m_CurLock.ClaimLock();

		if (m_iCurSize < m_iSize)
		{
			//put the request into the buffer
			m_buffer[m_iCurPlaceIndex] = elem;	//invoke assignment operator
			m_iCurPlaceIndex = (m_iCurPlaceIndex+1) % m_iSize;
			++m_iCurSize;	//just added an element

			iRet = m_iCurSize;
		}
		else
		{
			iRet = eEnqueueFailed;	// could not insert
		}

		m_CurLock.ReleaseLock();

		return iRet;
	}

	//returns the index position of the element removed
	//if the queue is empty, returns zero
	int dequeue(T &elem)
	{
		int iRet;

		m_CurLock.ClaimLock();

		if (m_iCurSize > 0)
		{
			//remove the request from the buffer
			elem = m_buffer[m_iCurPickIndex];	//invoke assignment operator			
			iRet = m_iCurPickIndex+1;	//return the element's index plus one as 0 is reserved as the failed return code
			m_iCurPickIndex = (m_iCurPickIndex+1) % m_iSize;
			--m_iCurSize;	//just removed an element			
		}
		else
		{
			iRet = eDequeueFailed;
		}

		m_CurLock.ReleaseLock();

		return iRet;
	}

	int size() { return m_iCurSize; }
	bool empty() { return (m_iCurSize == 0); }
	bool full() {return (m_iCurSize == m_iSize); }
};

}	// namespace TPCE

#endif //QUEUE_H
