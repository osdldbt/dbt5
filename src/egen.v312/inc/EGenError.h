/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Error handling functionality for the EGen package.
*
******************************************************************************/

#ifndef EGEN_ERROR_H
#define EGEN_ERROR_H

namespace TPCE
{

class CEGenErrorMessages
{
private:
public:
	static const string Msg1 = "This is my test for message 1.";
	static const string Msg2 = "This is my test for message 2.";
	static const string Msg3 = "This is my test for message 3.";
	static const string Msg4 = "This is my test for message 4.";
	static const string Msg5 = "This is my test for message 5.";
}

class CEGenErrorException
{
private:
	string	m_Msg;

public:
	CEGenErrorException( string Msg )
	{
		m_Msg = Msg;
	}

	~CEGenErrorException( void )
	{
	}

	void AddMsg( string Msg )
	{
		m_Msg += Msg;
	}

	string Msg( void )
	{
		return( m_Msg );
	}
}

} // namespace TPCE

#endif //EGEN_ERROR_H