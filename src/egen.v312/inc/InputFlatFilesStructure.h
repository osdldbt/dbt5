/******************************************************************************
*	(c) Copyright 2005, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		Matt Emmerton, IBM
*
*	Description:		Superstructure that contains all the input flat files used
*						by the loader and the driver.
*
******************************************************************************/

#ifndef INPUT_FLAT_FILE_STRUCTURE_H
#define INPUT_FLAT_FILE_STRUCTURE_H

namespace TPCE
{

enum eDriverType
{
  eDriverEGenLoader,
  eDriverCE,
  eDriverMEE,
  eDriverDM
};

enum eOutputVerbosity
{
  eOutputQuiet,
  eOutputVerbose
};

//Pointers to all the input files structure
class CInputFiles
{
  public:
	TAreaCodeFile				*AreaCodes;
	TChargeFile				*Charge;
	TCommissionRateFile			*CommissionRate;
	CCompanyFile				*Company;
	CCompanyCompetitorFile			*CompanyCompetitor;
	TCompanySPRateFile			*CompanySPRate;
	TExchangeFile				*Exchange;
	TFemaleFirstNamesFile			*FemaleFirstNames;
	TIndustryFile				*Industry;
	TLastNamesFile				*LastNames;
	TMaleFirstNamesFile			*MaleFirstNames;
	TNewsFile				*News;
	TSectorFile				*Sectors;
	CSecurityFile				*Securities;
	TStatusTypeFile				*StatusType;
	TStreetNamesFile			*Street;
	TStreetSuffixFile			*StreetSuffix;
	TTaxableAccountNameFile			*TaxableAccountName;
	TNonTaxableAccountNameFile		*NonTaxableAccountName;
	TTaxRatesCountryFile			*TaxRatesCountry;
	TTaxRatesDivisionFile			*TaxRatesDivision;
	TTradeTypeFile				*TradeType;
	TZipCodeFile				*ZipCode;

	CInputFiles() {};
	~CInputFiles() {};

        bool Initialize(eDriverType eType, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, char *szPathName);
};

}	// namespace TPCE

#endif // INPUT_FLAT_FILE_STRUCTURE_H
