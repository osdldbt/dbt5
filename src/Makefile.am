# Makefile
# 2006 Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

INCLUDES = -I@egen_path@/inc -Iinclude

AM_CPPFLAGS=#-ggdb
DEFS=-DCOMPILE_PLSQL_FUNCTION #-DDEBUG

TXN=transactions
INTF=interfaces
EGEN=EGen_v3.14/src

bin_PROGRAMS = TestTxn BrokerageHouseMain DriverCustomerMain DriverMarketMain

TestTxn_SOURCES = $(EGEN)/ReadRowFunctions.cpp $(EGEN)/InputFlatFilesStructure.cpp $(EGEN)/error.cpp $(EGEN)/EGenVersion.cpp $(EGEN)/AddressTable.cpp $(EGEN)/CustomerTable.cpp $(EGEN)/CustomerSelection.cpp $(EGEN)/Random.cpp $(EGEN)/Person.cpp $(EGEN)/DateTime.cpp $(EGEN)/CETxnInputGenerator.cpp $(EGEN)/DM.cpp $(EGEN)/WheelTime.cpp $(EGEN)/MEESecurity.cpp $(EGEN)/MEEPriceBoard.cpp $(EGEN)/MEETickerTape.cpp $(EGEN)/MEETradingFloor.cpp $(EGEN)/MEE.cpp $(TXN)/DBConnection.cpp $(TXN)/TxnBaseDB.cpp $(INTF)/MEESUTtest.cpp $(INTF)/DMSUTtest.cpp $(TXN)/MarketFeedDB.cpp $(TXN)/TradeCleanupDB.cpp $(TXN)/DataMaintenanceDB.cpp $(TXN)/MarketWatchDB.cpp $(TXN)/SecurityDetailDB.cpp $(TXN)/BrokerVolumeDB.cpp $(TXN)/CustomerPositionDB.cpp $(TXN)/TradeUpdateDB.cpp $(TXN)/TradeLookupDB.cpp $(TXN)/TradeResultDB.cpp $(TXN)/TradeOrderDB.cpp $(TXN)/TradeStatusDB.cpp $(INTF)/TxnHarnessSendToMarketTest.cpp TestTransactions/TestTxn.cpp

BrokerageHouseMain_SOURCES = $(EGEN)/DateTime.cpp $(INTF)/CSocket.cpp $(INTF)/TxnHarnessSendToMarket.cpp  BrokerageHouse/BrokerageHouseMain.cpp $(TXN)/DBConnection.cpp $(TXN)/TxnBaseDB.cpp $(TXN)/MarketFeedDB.cpp $(TXN)/TradeCleanupDB.cpp $(TXN)/DataMaintenanceDB.cpp $(TXN)/MarketWatchDB.cpp $(TXN)/SecurityDetailDB.cpp $(TXN)/BrokerVolumeDB.cpp $(TXN)/CustomerPositionDB.cpp $(TXN)/TradeUpdateDB.cpp $(TXN)/TradeLookupDB.cpp $(TXN)/TradeResultDB.cpp $(TXN)/TradeOrderDB.cpp $(TXN)/TradeStatusDB.cpp BrokerageHouse/BrokerageHouse.cpp

DriverCustomerMain_SOURCES = DriverCustomer/DriverCustomer.cpp DriverCustomer/DriverCustomerMain.cpp $(EGEN)/CETxnMixGenerator.cpp $(EGEN)/CETxnInputGenerator.cpp   $(EGEN)/ReadRowFunctions.cpp $(EGEN)/InputFlatFilesStructure.cpp $(EGEN)/error.cpp $(EGEN)/AddressTable.cpp $(EGEN)/EGenVersion.cpp $(EGEN)/CustomerTable.cpp $(EGEN)/CustomerSelection.cpp $(EGEN)/Random.cpp $(EGEN)/Person.cpp $(EGEN)/DateTime.cpp $(EGEN)/CE.cpp $(INTF)/CESUT.cpp $(INTF)/CSocket.cpp

DriverMarketMain_SOURCES = $(EGEN)/ReadRowFunctions.cpp $(EGEN)/EGenVersion.cpp $(EGEN)/error.cpp $(INTF)/CSocket.cpp $(EGEN)/Random.cpp $(EGEN)/DateTime.cpp DriverMarket/DriverMarket.cpp DriverMarket/DriverMarketMain.cpp $(EGEN)/WheelTime.cpp $(EGEN)/MEESecurity.cpp $(EGEN)/MEEPriceBoard.cpp $(EGEN)/MEETickerTape.cpp $(EGEN)/MEETradingFloor.cpp $(EGEN)/MEE.cpp $(INTF)/MEESUT.cpp
