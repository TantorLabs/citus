# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

Makefile.global:
	./configure

include Makefile.global

all: columnar


# build columnar only
columnar:
	$(MAKE) -C src/backend/columnar all

install:
	$(MAKE) -C src/backend/columnar install

clean:
	$(MAKE) -C src/backend/columnar/ clean

.PHONY: all check clean install
