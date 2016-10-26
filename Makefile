EXTENSION    = pg_timelord
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test
DOCS         = $(wildcard README.md)

PG_CONFIG = pg_config

MODULE_big = pg_timelord
OBJS = pg_timelord.o

all:

release-zip: all
	git archive --format zip --prefix=pg_timelord-${EXTVERSION}/ --output ./pg_timelord-${EXTVERSION}.zip HEAD
	unzip ./pg_timelord-$(EXTVERSION).zip
	rm ./pg_timelord-$(EXTVERSION).zip
	rm ./pg_timelord-$(EXTVERSION)/.gitignore
	sed -i -e "s/__VERSION__/$(EXTVERSION)/g"  ./pg_timelord-$(EXTVERSION)/META.json
	zip -r ./pg_timelord-$(EXTVERSION).zip ./pg_timelord-$(EXTVERSION)/
	rm ./pg_timelord-$(EXTVERSION) -rf


DATA = $(wildcard *--*.sql)
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
