add_subdirectory (custom)

install (FILES BaseInterface.h
               BrokerageHouse.h
               BrokerVolumeDB.h
               CESUT.h
               CommonStructs.h
               CSocket.h
               CThreadErr.h
               Customer.h
               CustomerPositionDB.h
               DataMaintenanceDB.h
               DBConnection.h
               DBT5Consts.h
               DMSUT.h
               DMSUTtest.h
               Driver.h
               MarketExchange.h
               MarketFeedDB.h
               MarketWatchDB.h
               MEESUT.h
               MEESUTtest.h
               SecurityDetailDB.h
               TradeCleanupDB.h
               TradeLookupDB.h
               TradeOrderDB.h
               TradeResultDB.h
               TradeStatusDB.h
               TradeUpdateDB.h
               TxnBaseDB.h
               TxnHarnessSendToMarket.h
               TxnHarnessSendToMarketTest.h
         DESTINATION "include/dbt5")
