MODULE_big = pg_timelord
OBJS = pg_timelord.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
