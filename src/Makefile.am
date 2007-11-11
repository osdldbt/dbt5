# Makefile
# 2006 Mark Wong & Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

DEFS=-D@STORED_PROC_TYPE@
INCLUDES=-I@egen_path@/inc -Iinclude @DB_CFLAGS@

TXN=transactions
INTF=interfaces
EGEN=@egen_path@/src

bin_PROGRAMS = BrokerageHouseMain DriverMain DriverMarketMain TestTxn

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
TestTxn_LDADD = $(DB_LDFLAGS)

BrokerageHouseMain_SOURCES = $(EGEN)/DateTime.cpp \
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
BrokerageHouseMain_LDADD = $(DB_LDFLAGS)

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
                             $(INTF)/CESUT.cpp \
                             $(INTF)/CSocket.cpp \
                             $(INTF)/DMSUT.cpp \
                             $(EGEN)/DM.cpp \
                             $(INTF)/BaseInterface.cpp
DriverMain_LDFLAGS = $(DB_LDFLAGS)

DriverMarketMain_SOURCES = $(EGEN)/ReadRowFunctions.cpp \
                           $(EGEN)/BaseLogger.cpp \
                           $(EGEN)/EGenLogFormatterTab.cpp \
                           $(EGEN)/EGenVersion.cpp \
                           $(EGEN)/error.cpp \
                           $(INTF)/CSocket.cpp \
                           $(EGEN)/Random.cpp \
                           $(EGEN)/DateTime.cpp \
                           DriverMarket/DriverMarket.cpp \
                           DriverMarket/DriverMarketMain.cpp \
                           $(EGEN)/WheelTime.cpp \
                           $(EGEN)/MEESecurity.cpp \
                           $(EGEN)/MEEPriceBoard.cpp \
                           $(EGEN)/MEETickerTape.cpp \
                           $(EGEN)/MEETradingFloor.cpp \
                           $(EGEN)/MEE.cpp \
                           $(INTF)/MEESUT.cpp \
                           $(INTF)/BaseInterface.cpp
DriverMarketMain_LDADD = $(DB_LDFLAGS)
