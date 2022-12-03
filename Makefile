# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

Makefile.global:
	./configure --without-libcurl

include Makefile.global

all: columnar


# build columnar only
columnar:
	$(MAKE) -C src/backend/columnar all

clean:
	$(MAKE) -C src/backend/columnar/ clean

install:
	$(MAKE) -C src/backend/columnar/ install

# depend on install-all so that downgrade scripts are installed as well
check: all
	$(MAKE) -C src/test/regress check-columnar

.PHONY: all check clean install
