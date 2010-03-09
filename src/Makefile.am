# Makefile
# 2006 Mark Wong & Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

DEFS=-D@STORED_PROC_TYPE@

TXN=transactions
INTF=interfaces
EGEN=@egen_path@/src

bin_PROGRAMS = BrokerageHouseMain DriverMain MarketExchangeMain TestTxn

TestTxn_CPPFLAGS=-I@egen_path@/inc -Iinclude @DB_CFLAGS@
TestTxn_LDFLAGS=$(DB_LDFLAGS)
TestTxn_SOURCES = $(EGEN)/ReadRowFunctions.cpp \
                  $(EGEN)/BaseLogger.cpp \
                  $(EGEN)/EGenLogFormatterTab.cpp \
                  $(EGEN)/InputFlatFilesStructure.cpp \
                  $(EGEN)/error.cpp \
                  $(EGEN)/EGenVersion.cpp \
                  $(EGEN)/AddressTable.cpp \
                  $(EGEN)/CustomerTable.cpp \
                  $(EGEN)/CustomerSelection.cpp \
                  $(EGEN)/Random.cpp \
                  $(EGEN)/Person.cpp \
                  $(EGEN)/DateTime.cpp \
                  $(EGEN)/CETxnInputGenerator.cpp \
                  $(EGEN)/DM.cpp \
                  $(EGEN)/WheelTime.cpp \
                  $(EGEN)/MEESecurity.cpp \
                  $(EGEN)/MEEPriceBoard.cpp \
                  $(EGEN)/MEETickerTape.cpp \
                  $(EGEN)/MEETradingFloor.cpp \
                  $(EGEN)/MEE.cpp \
                  $(EGEN)/locking.cpp \
                  $(TXN)/DBConnection.cpp \
                  $(TXN)/TxnBaseDB.cpp \
                  $(INTF)/MEESUTtest.cpp \
                  $(INTF)/DMSUTtest.cpp \
                  $(TXN)/MarketFeedDB.cpp \
                  $(TXN)/TradeCleanupDB.cpp \
                  $(TXN)/DataMaintenanceDB.cpp \
                  $(TXN)/MarketWatchDB.cpp \
                  $(TXN)/SecurityDetailDB.cpp \
                  $(TXN)/BrokerVolumeDB.cpp \
                  $(TXN)/CustomerPositionDB.cpp \
                  $(TXN)/TradeUpdateDB.cpp \
                  $(TXN)/TradeLookupDB.cpp \
                  $(TXN)/TradeResultDB.cpp \
                  $(TXN)/TradeOrderDB.cpp \
                  $(TXN)/TradeStatusDB.cpp \
                  $(INTF)/BaseInterface.cpp \
                  $(INTF)/CESUT.cpp \
                  $(INTF)/CSocket.cpp \
                  $(INTF)/TxnHarnessSendToMarketTest.cpp \
                  TestTransactions/TestTxn.cpp

BrokerageHouseMain_CPPFLAGS=-I@egen_path@/inc -Iinclude @DB_CFLAGS@
BrokerageHouseMain_LDFLAGS=$(DB_LDFLAGS)
BrokerageHouseMain_SOURCES = $(EGEN)/DateTime.cpp \
                             $(EGEN)/locking.cpp \
                             $(INTF)/CSocket.cpp \
                             $(INTF)/TxnHarnessSendToMarket.cpp \
                             BrokerageHouse/BrokerageHouseMain.cpp \
                             $(TXN)/DBConnection.cpp \
                             $(TXN)/TxnBaseDB.cpp \
                             $(TXN)/MarketFeedDB.cpp \
                             $(TXN)/TradeCleanupDB.cpp \
                             $(TXN)/DataMaintenanceDB.cpp \
                             $(TXN)/MarketWatchDB.cpp \
                             $(TXN)/SecurityDetailDB.cpp \
                             $(TXN)/BrokerVolumeDB.cpp \
                             $(TXN)/CustomerPositionDB.cpp \
                             $(TXN)/TradeUpdateDB.cpp \
                             $(TXN)/TradeLookupDB.cpp \
                             $(TXN)/TradeResultDB.cpp \
                             $(TXN)/TradeOrderDB.cpp \
                             $(TXN)/TradeStatusDB.cpp \
                             BrokerageHouse/BrokerageHouse.cpp

DriverMain_CPPFLAGS=-I@egen_path@/inc -Iinclude @DB_CFLAGS@
DriverMain_LDFLAGS=$(DB_LDFLAGS)
DriverMain_SOURCES = Driver/Driver.cpp \
                             Driver/DriverMain.cpp \
                             Customer/Customer.cpp \
                             $(EGEN)/BaseLogger.cpp \
                             $(EGEN)/EGenLogFormatterTab.cpp \
                             $(EGEN)/CETxnMixGenerator.cpp \
                             $(EGEN)/CETxnInputGenerator.cpp \
                             $(EGEN)/ReadRowFunctions.cpp \
                             $(EGEN)/InputFlatFilesStructure.cpp \
                             $(EGEN)/error.cpp \
                             $(EGEN)/AddressTable.cpp \
                             $(EGEN)/EGenVersion.cpp \
                             $(EGEN)/CustomerTable.cpp \
                             $(EGEN)/CustomerSelection.cpp \
                             $(EGEN)/Random.cpp \
                             $(EGEN)/Person.cpp \
                             $(EGEN)/DateTime.cpp \
                             $(EGEN)/CE.cpp \
                             $(EGEN)/DM.cpp \
                             $(EGEN)/locking.cpp \
                             $(EGEN)/MEE.cpp \
                             $(EGEN)/MEETradingFloor.cpp \
                             $(EGEN)/MEETickerTape.cpp \
                             $(EGEN)/MEEPriceBoard.cpp \
                             $(EGEN)/WheelTime.cpp \
                             $(EGEN)/MEESecurity.cpp \
                             $(INTF)/CESUT.cpp \
                             $(INTF)/CSocket.cpp \
                             $(INTF)/DMSUT.cpp \
                             $(INTF)/BaseInterface.cpp

MarketExchangeMain_CPPFLAGS=-I@egen_path@/inc \
	-I@egen_path@/TestHarness/Reference/inc -Iinclude @DB_CFLAGS@
MarketExchangeMain_LDFLAGS=$(DB_LDFLAGS)
MarketExchangeMain_SOURCES = $(EGEN)/ReadRowFunctions.cpp \
                           $(EGEN)/BaseLogger.cpp \
                           $(EGEN)/EGenLogFormatterTab.cpp \
                           $(EGEN)/EGenVersion.cpp \
                           $(EGEN)/error.cpp \
                           $(INTF)/CSocket.cpp \
                           $(EGEN)/Random.cpp \
                           $(EGEN)/DateTime.cpp \
                           MarketExchange/MarketExchange.cpp \
                           MarketExchange/MarketExchangeMain.cpp \
                           $(EGEN)/WheelTime.cpp \
                           $(EGEN)/MEESecurity.cpp \
                           $(EGEN)/MEEPriceBoard.cpp \
                           $(EGEN)/MEETickerTape.cpp \
                           $(EGEN)/MEETradingFloor.cpp \
                           $(EGEN)/MEE.cpp \
                           $(EGEN)/locking.cpp \
                           $(EGEN)/InputFlatFilesStructure.cpp \
                           $(INTF)/MEESUT.cpp \
                           $(INTF)/BaseInterface.cpp
