/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Contains all seeds used with the Random Number Generator (RNG).
*/

#ifndef RNGSEEDS_H
#define RNGSEEDS_H

namespace TPCE
{
	// Default seed used for all tables.
	const RNGSEED	RNGSeedTableDefault = 37039940;

	// This value is added to the AD_ID when seeding the RNG for
	// generating a threshold into the TownDivisionZipCode list.
	const RNGSEED	RNGSeedBaseTownDivZip = 26778071;

	// This is the base seed used when generating C_TIER.
	const RNGSEED	RNGSeedBaseC_TIER = 16225173;

	// Base seeds used for generating C_AREA_1, C_AREA_2, C_AREA_3
	const RNGSEED	RNGSeedBaseC_AREA_1 = 97905013;
	const RNGSEED	RNGSeedBaseC_AREA_2 = 68856487;
	const RNGSEED	RNGSeedBaseC_AREA_3 = 67142295;

	// Base seed used when generating names.
	const RNGSEED	RNGSeedBaseFirstName = 95066470;
	const RNGSEED	RNGSeedBaseMiddleInitial = 71434514;
	const RNGSEED	RNGSeedBaseLastName = 35846049;

	// Base seed used when generating gender.
	const RNGSEED	RNGSeedBaseGender = 9568922;

	// Base seed used when generating tax ID
	const RNGSEED	RNGSeedBaseTaxID = 8731255;

	// Base seed used when generating the number of accounts for a customer
	const RNGSEED	RNGSeedBaseNumberOfAccounts = 37486207;

	// Base seed used when generating the number of permissions on an account
	const RNGSEED	RNGSeedBaseNumberOfAccountPermissions = 27794203;

	// Base seeds used when generating CIDs for additional account permissions
	const RNGSEED	RNGSeedBaseCIDForPermission1 = 76103629;
	const RNGSEED	RNGSeedBaseCIDForPermission2 = 103275149;

	// Base seed used when generating acount tax status
	const RNGSEED	RNGSeedBaseAccountTaxStatus = 34376701;

	// Base seed for determining account broker id
	const RNGSEED	RNGSeedBaseBrokerId = 75607774;

	// Base seed used when generating tax rate row
	const RNGSEED	RNGSeedBaseTaxRateRow = 92740731;

	// Base seed used when generating the number of holdings for an account
	const RNGSEED	RNGSeedBaseNumberOfSecurities = 23361736;

	// Base seed used when generating the starting security ID for the 
	// set of securities associated with a particular account.
	const RNGSEED	RNGSeedBaseStartingSecurityID = 12020070;

	// Base seed used when generating a company's SP Rate
	const RNGSEED	RNGSeedBaseSPRate = 56593330;

	// Base seed for initial trade generation class
	const RNGSEED	RNGSeedTradeGen = 32900134;

	// Base seed for the MEESecurity class
	const RNGSEED	RNGSeedBaseMEESecurity = 75791232;

	// Base seed for non-uniform customer selection
	const RNGSEED	RNGSeedCustomerSelection = 9270899;

	// Base seed for MEE Ticker Tape
	const RNGSEED	RNGSeedBaseMEETickerTape = 42065035;

	// Base seed for MEE Trading Floor
	const RNGSEED	RNGSeedBaseMEETradingFloor = 25730774;

	// Base seed for TxnMixGenerator
	const RNGSEED	RNGSeedBaseTxnMixGenerator = 87944308;

	// Base seed for TxnInputGenerator
	const RNGSEED	RNGSeedBaseTxnInputGenerator = 80534927;

}	// namespace TPCE

#endif //RNGSEEDS_H
