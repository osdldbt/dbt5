/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Table column length constants used by the loader and
*	transactions.
*/
#ifndef TABLE_CONSTS_H
#define TABLE_CONSTS_H

namespace TPCE
{

// length of character columns used in both ADDRESS and ZIP_CODE tables
const int cTOWN_len	= 80;
const int cDIV_len	= 80;
const int cCODE_len	= 12;

//BROKER table
const int cB_NAME_len = 100;

//ACCOUNT_PERMISSION table 
const int cACL_len = 4;

//length of character columns in ADDRESS table
const int cAD_NAME_len	= 80;
const int cAD_LINE_len = 80;
const int cAD_TOWN_len	= cTOWN_len;
const int cAD_DIV_len = cDIV_len;	//state/provice abreviation
const int cAD_ZIP_len = cCODE_len;
const int cAD_CTRY_len = 80;

//CASH_TRANSACTION table
const int cCT_NAME_len = 100;

//COMPANY table
const int cCO_NAME_len = 60;
const int cSP_RATE_len = 4;
const int cCEO_NAME_len = 100;
const int cCO_DESC_len = 150;
const int cCO_SP_RATE_len = 4;

//CUSTOMER table
const int cL_NAME_len		= 30;
const int cF_NAME_len		= 30;
const int cM_NAME_len		= 1;
const int cDOB_len		= 30;
const int cTAX_ID_len		= 20;
const int cGNDR_len		= 1;
const int cCTRY_len	= 3;
const int cAREA_len	= 3;
const int cLOCAL_len	= 10;
const int cEXT_len	= 5;
const int cEMAIL_len	= 50;

//CUSTOMER_ACCOUNT table
const int cCA_NAME_len		= 50;

//EXCHANGE table
const int cEX_ID_len = 6;
const int cEX_NAME_len = 100;
const int cEX_DESC_len = 150;
//const int cEX_OPEN_len = 8;
//const int cEX_CLOSE_len = 8;

//HOLDING table
const int cH_BUY_DTS_len = 30;	//date of purchase

//INDUSTRY table
const int cIN_ID_len = 2;	
const int cIN_NAME_len = 50;

//NEWS_ITEM table
const int cNI_HEADLINE_len = 80;
const int cNI_SUMMARY_len = 255;
const int cNI_ITEM_len = 100 * 1000;
const int cNI_SOURCE_len = 30;
const int cNI_AUTHOR_len = 30;

//SECURITY table
const int cS_NAME_len = 70;
const int cSYMBOL_len = 7 + 1 + 7;	// base + separator + extended
const int cS_ISSUE_len = 6;

//SETTLEMENT table
const int cSE_CASH_TYPE_len = 40;

//SECTOR table
const int cSC_NAME_len = 30;
const int cSC_ID_len = 2;

//STATUS_TYPE table
const int cST_ID_len = 4;
const int cST_NAME_len = 30;

//TAX RATE table
const int cTX_ID_len = 4;
const int cTX_NAME_len = 50;

//TRADE table
const int cEXEC_NAME_len = cF_NAME_len + cM_NAME_len + cL_NAME_len + 3;

//TRADE_HISTORY table
const int cTH_ST_ID_len = cST_ID_len;

//TRADE TYPE table
const int cTT_ID_len = 3;
const int cTT_NAME_len = 30;

//ZIP_CODE table
const int cZC_TOWN_len = cTOWN_len;
const int cZC_DIV_len = cDIV_len;
const int cZC_CODE_len = cCODE_len;

}	// namespace TPCE

#endif //TABLE_CONSTS_H
