# Makefile for the Trade Status txn test
# 2006 Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

INCLUDES = -Iinclude

AM_CPPFLAGS=-ggdb
DEFS=-DCOMPILE_PLSQL_FUNCTION -DDEBUG

TXN=transactions
EGEN=EGen_v3.14/src

bin_PROGRAMS = TestTxn

TestTxn_SOURCES = $(EGEN)/ReadRowFunctions.cpp $(EGEN)/InputFlatFilesStructure.cpp $(EGEN)/error.cpp $(EGEN)/EGenVersion.cpp $(EGEN)/AddressTable.cpp $(EGEN)/CustomerTable.cpp $(EGEN)/CustomerSelection.cpp $(EGEN)/Random.cpp $(EGEN)/Person.cpp $(EGEN)/DateTime.cpp $(EGEN)/CETxnInputGenerator.cpp $(TXN)/DBConnection.cpp $(TXN)/TxnBaseDB.cpp $(TXN)/TradeResultDB.cpp $(TXN)/TradeOrderDB.cpp $(TXN)/TradeStatusDB.cpp $(TXN)/TxnHarnessSendToMarket.cpp TestTransactions/TestTxn.cpp
