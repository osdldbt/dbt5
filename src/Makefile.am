# Makefile for the Trade Status txn test
# 2006 Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

INCLUDES = -Iinclude

APPDEFINES=-DCOMPILE_PLSQL_FUNCTION

TXN=transactions

bin_PROGRAMS = testTS

testTS_SOURCES = $(TXN)/DBConnection.cpp $(TXN)/TxnBaseDB.cpp $(TXN)/TradeStatusDB.cpp TestTradeStatus/TradeStatusTest.cpp
