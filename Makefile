# Citus toplevel Makefile

citus_subdir = .
citus_top_builddir = .
extension_dir = $(shell $(PG_CONFIG) --sharedir)/extension

# Hint that configure should be run first
ifeq (,$(wildcard Makefile.global))
  $(error ./configure needs to be run before compiling Citus)
endif

include Makefile.global

all: columnar


# build columnar only
columnar:
	./configure
	$(MAKE) -C src/backend/columnar all

install: columnar
	$(MAKE) -C src/backend/columnar install

clean:
	$(MAKE) -C src/backend/columnar/ clean

.PHONY: all check clean install
