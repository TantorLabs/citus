/*-------------------------------------------------------------------------
 *
 * citus_depended_object.h
 *   Hide citus objects.
 *
 * Copyright (c) CitusDependent Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_DEPENDED_OBJECT_H
#define CITUS_DEPENDED_OBJECT_H

#include "catalog/objectaddress.h"
#include "distributed/commands.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "postgres_ext.h"
#include "utils/hsearch.h"

extern bool HideCitusDependentObjects;

extern bool HideCitusDependentObjectsFromPgMetaTable(Node *node, void *context);
extern bool IsPgLocksTable(RangeTblEntry *rte);
extern bool IsCitusDependentObject(ObjectAddress objectAddress, HTAB *dependentObjects);

#endif /* CITUS_DEPENDED_OBJECT_H */
