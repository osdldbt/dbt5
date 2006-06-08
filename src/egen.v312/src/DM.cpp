/******************************************************************************
*	(c) Copyright 2005, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Implementation of the DM class.
*						See DM.h for a description.
*
******************************************************************************/

#include "../inc/DM.h"

using namespace TPCE;

const INT32		iDataMaintenanceTableCount = 12;
static char*	DataMaintenanceTableName[iDataMaintenanceTableCount] = 
				{ (char *) "ACCOUNT_PERMISSION",
				  (char *) "ADDRESS",
				  (char *) "COMPANY",
				  (char *) "CUSTOMER",
				  (char *) "CUSTOMER_TAXRATE",
				  (char *) "DAILY_MARKET",
				  (char *) "EXCHANGE",
				  (char *) "FINANCIAL",
				  (char *) "NEWS_ITEM",
				  (char *) "SECURITY",
				  (char *) "TAXRATE",
				  (char *) "WATCH_ITEM" };

// Automatically generate unique RNG seeds.
// The CRandom class uses an unsigned 64-bit value for the seed.
// This routine automatically generates two unique seeds. One is used for
// the TxnInput generator RNG, and the other is for the TxnMixGenerator RNG.
// The 64 bits are used as follows.
//
//	Bits	0 - 31	Caller provided unique unsigned 32-bit id.
//	Bit		32		0
//	Bits	33 - 43	Number of days since the base time. The base time
//					is set to be January 1 of the most recent year that is
//					a multiple of 5. This allows enough space for the last
//					field, and it makes the algorithm "timeless" by resetting
//					the generated values every 5 years.
//  Bits	44 - 63	Current time of day measured in 1/10's of a second.
//
void CDM::AutoSetRNGSeeds( UINT32 UniqueId )
{
	CDateTime	Now;
	INT32		BaseYear;
	INT32		Tmp1, Tmp2;

	Now.GetYMD( &BaseYear, &Tmp1, &Tmp2 );

	// Set the base year to be the most recent year that was a multiple of 5.
	BaseYear -= ( BaseYear % 5 );
	CDateTime	Base( BaseYear, 1, 1 );	// January 1st in the BaseYear

	// Initialize the seed with the current time of day measured in 1/10's of a second.
	// This will use up to 20 bits.
	RNGSEED	Seed;
	Seed = Now.MSec() / 100;

	// Now add in the number of days since the base time.
	// The number of days in the 5 year period requires 11 bits.
	// So shift up by that much to make room in the "lower" bits.
	Seed <<= 11;
	Seed += Now.DayNo() - Base.DayNo();

	// So far, we've used up 31 bits.
	// Save the "last" bit of the "upper" 32 for the RNG id. In
	// this case, it is always 0 since we don't have a second
	// RNG in this class.
	// In addition, make room for the caller's 32-bit unique id.
	// So shift a total of 33 bits.
	Seed <<= 33;

	// Now the "upper" 32-bits have been set with a value for RNG 0.
	// Add in the sponsor's unique id for the "lower" 32-bits.
	Seed += UniqueId;

	// Set the TxnMixGenerator RNG to the unique seed.
	m_rnd.SetSeed( Seed );
}

void CDM::Initialize( void )
{
	m_iSecurityCount = m_pSecurities->GetActiveSecurityCount();
	m_iCompanyCount = m_pCompanies->GetActiveCompanyCount();
	m_iDivisionTaxCount = m_pTaxRatesDivision->GetSize();
}

CDM::CDM( CDMSUTInterface *pSUT, CInputFiles &inputFiles, TIdent iActiveCustomerCount, UINT32 UniqueId )
	: m_iActiveCustomerCount(iActiveCustomerCount)
	, m_pSecurities(inputFiles.Securities)
	, m_pCompanies(inputFiles.Company)
	, m_pTaxRatesDivision(inputFiles.TaxRatesDivision)
	, m_DataMaintenanceTableNum(0)
	, m_iDivisionTaxCount(0)
	, m_pSUT( pSUT )
{
	AutoSetRNGSeeds( UniqueId );
	Initialize();
}

CDM::CDM( CDMSUTInterface *pSUT, CInputFiles &inputFiles, TIdent iActiveCustomerCount, RNGSEED RNGSeed )
	: m_rnd(RNGSeed)	//to be predictable
	, m_iActiveCustomerCount(iActiveCustomerCount)
	, m_pSecurities(inputFiles.Securities)
	, m_pCompanies(inputFiles.Company)
	, m_pTaxRatesDivision(inputFiles.TaxRatesDivision)
	, m_DataMaintenanceTableNum(0)
	, m_iDivisionTaxCount(0)
	, m_pSUT( pSUT )
{
	Initialize();
}

TIdent CDM::GenerateRandomCustomerId()
{
	return m_rnd.RndInt64Range(iDefaultStartFromCustomer, 
							 iDefaultStartFromCustomer + m_iActiveCustomerCount - 1);
}

TIdent CDM::GenerateRandomCompanyId()
{
	return m_rnd.RndInt64Range(1, m_iCompanyCount);
}

TIdent CDM::GenerateRandomSecurityId()
{
	return m_rnd.RndInt64Range(0, m_iSecurityCount-1);
}

RNGSEED CDM::GetRNGSeed( void )
{
	return( m_rnd.GetSeed() );
}

void CDM::DoTxn( void )
{
	memset( &m_TxnInput, 0, sizeof( m_TxnInput ));
	strncpy( m_TxnInput.table_name, 
			DataMaintenanceTableName[m_DataMaintenanceTableNum],
			sizeof(m_TxnInput.table_name ));

	switch( m_DataMaintenanceTableNum )
	{
	case 0:	// ACCOUNT_PERMISSION
		m_TxnInput.c_id = GenerateRandomCustomerId();
		break;
	case 1: // ADDRESS
		m_TxnInput.c_id = GenerateRandomCustomerId();
		break;
	case 2: // COMPANY
		m_TxnInput.co_id = GenerateRandomCompanyId();
		break;
	case 3: // CUSTOMER
		m_TxnInput.c_id = GenerateRandomCustomerId();
		break;
	case 4: // CUSTOMER_TAXRATE
		m_TxnInput.c_id = GenerateRandomCustomerId();
		break;
	case 5: // DAILY_MARKET
		m_pSecurities->CreateSymbol( GenerateRandomSecurityId(), m_TxnInput.symbol, sizeof( m_TxnInput.symbol ));
		m_TxnInput.day_of_month = m_rnd.RndIntRange(1, 31);
		m_TxnInput.add_flag = m_rnd.RndIntRange(0, 1);
		break;
	case 6: // EXCHANGE
		break;
	case 7: // FINANCIAL
		m_TxnInput.co_id = GenerateRandomCompanyId();
		break;
	case 8: // NEWS_ITEM
		m_TxnInput.co_id = GenerateRandomCompanyId();
		break;
	case 9: // SECURITY
		m_pSecurities->CreateSymbol( GenerateRandomSecurityId(), m_TxnInput.symbol, sizeof( m_TxnInput.symbol ));
		break;
	case 10: // TAXRATE
		const vector<TTaxRateInputRow>	*pRates;
		INT32							iThreshold;

		pRates = m_pTaxRatesDivision->GetRecord(m_rnd.RndIntRange(0, m_iDivisionTaxCount - 1));
		iThreshold = m_rnd.RndIntRange(0, (INT32)pRates->size()-1);

		strncpy(m_TxnInput.tx_id, 
				(*pRates)[iThreshold].TAX_ID, 
				sizeof(m_TxnInput.tx_id));
		break;
	case 11: // WATCH_ITEM
		m_TxnInput.c_id = GenerateRandomCustomerId();
		break;

	default:
		assert(false);	// should never happen
	}

	m_pSUT->DataMaintenance( &m_TxnInput );

	m_DataMaintenanceTableNum = (m_DataMaintenanceTableNum + 1) % iDataMaintenanceTableCount;
}
