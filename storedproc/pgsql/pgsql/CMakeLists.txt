install (FILES broker_volume.sql
               customer_position.sql
               data_maintenance.sql
               market_watch.sql
               security_detail.sql
               trade_lookup.sql
               trade_order.sql
               trade_result.sql
               trade_status.sql
               trade_update.sql
        DESTINATION "share/dbt5/postgresql")
