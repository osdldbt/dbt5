# Default: build all procedures. Override from command line or environment, e.g.:
#   make MODULES="broker_volume trade_order"
#   STORED_PROCS="broker_volume trade_order" make
MODULES ?= broker_volume customer_position market_watch \
		security_detail trade_lookup trade_order trade_result trade_status \
		trade_update data_maintenance
ifdef STORED_PROCS
MODULES := $(STORED_PROCS)
endif
DATA_built = $(MODULES:=.sql)

PG_CPPFLAGS=-g
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
