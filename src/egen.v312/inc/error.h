/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*/
#ifndef ERROR_H
#define ERROR_H

#ifndef _INC_STRING
	#include <string.h>
#endif

namespace TPCE
{

const int m_szMsg_size = 512;
const int m_szLoc_size = 64;

#define ERR_TYPE_LOGIC							-1		//logic error in program; internal error
#define ERR_SUCCESS								0		//success (a non-error error)
#define	ERR_TYPE_ODBC							6		//odbc generated error
#define ERR_TYPE_OS								11		//operating system error
#define ERR_TYPE_MEMORY							12		//memory allocation error
#define ERR_TYPE_FIXED_MAP						27		//Error from CFixedMap
#define ERR_TYPE_FIXED_ARRAY					28		//Error from CFixedArray

#define ERR_INS_MEMORY			"Insufficient Memory to continue."
#define ERR_UNKNOWN				"Unknown error."
#define ERR_MSG_BUF_SIZE		512
#define INV_ERROR_CODE			-1

class CBaseErr
{
public:
	
	CBaseErr(char const * szLoc = NULL)
	{
		m_idMsg		= INV_ERROR_CODE;
		
		
		if (szLoc)
		{
			m_szLoc = new char[m_szLoc_size];
			strcpy(m_szLoc, szLoc);
		}
		else
			m_szLoc	= NULL;		
	}


	

	CBaseErr(int idMsg, char const * szLoc = NULL)
	{
		m_idMsg		= idMsg;
	

		if (szLoc)
		{
			m_szLoc = new char[m_szLoc_size];
			strcpy(m_szLoc, szLoc);
		}
		else
			m_szLoc	= NULL;		
	}
	
	virtual ~CBaseErr(void)
	{
		if (m_szLoc)
			delete [] m_szLoc;
	};

	char *GetLocation(void) { return m_szLoc; }
	virtual int ErrorNum() { return m_idMsg; }
	virtual int ErrorType() = 0;	// a value which distinguishes the kind of error that occurred
	virtual char *ErrorText() = 0;	// a string (i.e., human readable) representation of the error
	virtual char *ErrorLoc() { return m_szLoc; }

protected:
	char	*m_szLoc;	// code location where the error occurred
	int		m_idMsg;
	
	//short	m_errType;
};

class CSystemErr : public CBaseErr
{
public:
	enum Action
	{
		eNone = 0,
		eTransactNamedPipe,
		eWaitNamedPipe,
		eSetNamedPipeHandleState,
		eCreateFile,
		eCreateProcess,
		eCallNamedPipe,
		eCreateEvent,
		eCreateThread,
		eVirtualAlloc,
		eReadFile = 10,
		eWriteFile,
		eMapViewOfFile,
		eCreateFileMapping,
		eInitializeSecurityDescriptor,
		eSetSecurityDescriptorDacl,
		eCreateNamedPipe,
		eConnectNamedPipe,
		eWaitForSingleObject,
		eRegOpenKeyEx,
		eRegQueryValueEx = 20,
		ebeginthread,
		eRegEnumValue,
		eRegSetValueEx,
		eRegCreateKeyEx,
		eWaitForMultipleObjects,
		eRegisterClassEx,
		eCreateWindow,
		eCreateSemaphore,
		eReleaseSemaphore,
		eFSeek,
		eFRead,
		eFWrite,
		eTmpFile,
		eSetFilePointer,
		eNew,
		eCloseHandle,
		eCreateMutex,
		eReleaseMutex
	};
	
			CSystemErr(Action eAction, char const * szLocation);
			CSystemErr(int iError, Action eAction, char const * szLocation);
	int		ErrorType() { return ERR_TYPE_OS;};
	char	*ErrorText(void);	

	Action	m_eAction;

private:
	char m_szMsg[ERR_MSG_BUF_SIZE];
};

class CMemoryErr : public CBaseErr
{
public:
	CMemoryErr();
	CMemoryErr(char const * szLoc)
	{
		if (szLoc)
		{
			m_szLoc = new char[m_szLoc_size];
			strcpy(m_szLoc, szLoc);
		}
		else
			m_szLoc	= NULL;
	}
	~CMemoryErr()
	{
		if (m_szLoc!=NULL)
			delete m_szLoc;
	}

	int ErrorType() {return ERR_TYPE_MEMORY;}
	char *ErrorText() {return (char*)ERR_INS_MEMORY;}
};

class CODBCERR : public CBaseErr
{
	public:
		enum ACTION
		{
			eNone, 
			eUnknown, 
			eAllocConn,			// error from SQLAllocConnect
			eAllocHandle,		// error from SQLAllocHandle
			eBcpBind,			// error from bcp_bind
			eBcpControl,		// error from bcp_control
			eBcpInit,			// error from bcp_init
			eBcpBatch,			// error from bcp_batch
			eBcpDone,			// error from bcp_done
			eConnOption,		// error from SQLSetConnectOption
			eConnect,			// error from SQLConnect
			eAllocStmt,			// error from SQLAllocStmt
			eExecDirect,		// error from SQLExecDirect
			eBindParam,			// error from SQLBindParameter
			eBindCol,			// error from SQLBindCol
			eFetch,				// error from SQLFetch
			eFetchScroll,		// error from SQLFetchScroll
			eMoreResults,		// error from SQLMoreResults
			ePrepare,			// error from SQLPrepare
			eExecute,			// error from SQLExecute
			eBcpSendrow,		// error from bcp_sendrow
			eSetConnectAttr,	// error from SQLSetConnectAttr
			eSetEnvAttr,		// error from SQLSetEnvAttr
			eSetStmtAttr,		// error from SQLSetStmtAttr
			eSetCursorName,		// error from SQLSetCursonName
			eSQLSetPos,			// error from SQLSetPos
			eEndTxn,			// error from SQLEndTxn
			eNumResultCols,		// error from SQLNumResultCols
			eCloseCursor		// error from SQLCloseCursor
		};

		CODBCERR(char const * szLoc = NULL)
			: CBaseErr(szLoc)

		{
			m_eAction = eNone;
			m_NativeError = 0;
			m_bDeadLock = false;
			m_odbcerrstr = NULL;
		};

		~CODBCERR()
		{
			if (m_odbcerrstr != NULL)
				delete [] m_odbcerrstr;
		};

		ACTION	m_eAction;
		int		m_NativeError;
		bool	m_bDeadLock;
		char   *m_odbcerrstr;

		int ErrorType() {return ERR_TYPE_ODBC;};
		int ErrorNum() {return m_NativeError;};
		char *ErrorText() {return m_odbcerrstr;};

};

class CBaseTxnErr
{
public:
	enum
	{
		SUCCESS = 0,
		//Trade Order errors		
		UNAUTHORIZED_EXECUTOR,
		ROLLBACK,	// return from TradeOrderFrame5 to indicate transaction rollback
		BAD_INPUT_DATA,
		Last
	}	mErrCode;
	
	static char* ErrorText(int code)
	{
		static	char	*szMsgs[CBaseTxnErr::Last+1] = {
			(char*)"Success",
			//Trade Order errors
			(char*)"Unauthorized executor",
			(char*)"Transaction rolled back",
			(char*)"Bad input data",
			(char*)"Error code above the range"
		};

		if (code > CBaseTxnErr::Last)
			return szMsgs[CBaseTxnErr::Last];
		else
			return szMsgs[code];
	}

};

}	// namespace TPCE

#endif //ERROR_H
