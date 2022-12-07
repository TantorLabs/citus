/*-------------------------------------------------------------------------
 *
 * alter_table.c
 *	  Routines related to the altering of tables.
 *
 *		There are three UDFs defined in this file:
 *		undistribute_table:
 *			Turns a distributed table to a local table
 *		alter_distributed_table:
 *			Alters distribution_column, shard_count or colocate_with
 *			properties of a distributed table
 *		alter_table_set_access_method:
 *			Changes the access method of a table
 *
 *		All three methods work in similar steps:
 *			- Create a new table the required way (with a different
 *			  shard count, distribution column, colocate with value,
 *			  access method or local)
 *			- Move everything to the new table
 *			- Drop the old one
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_am.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_rewrite_d.h"
#include "columnar/columnar.h"
#include "columnar/columnar_tableam.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* Table Conversion Types */
#define ALTER_TABLE_SET_ACCESS_METHOD 'm'


typedef TableConversionReturn *(*TableConversionFunction)(struct
														  TableConversionParameters *);


/*
 * TableConversionState objects are used for table conversion functions:
 * UndistributeTable, AlterDistributedTable, AlterTableSetAccessMethod.
 *
 * They can be created using TableConversionParameters objects with
 * CreateTableConversion function.
 *
 * TableConversionState objects include everything TableConversionParameters
 * objects do and some extra to be used in the conversion process.
 */
typedef struct TableConversionState
{
	/*
	 * Determines type of conversion: UNDISTRIBUTE_TABLE,
	 * ALTER_DISTRIBUTED_TABLE, ALTER_TABLE_SET_ACCESS_METHOD.
	 */
	char conversionType;

	/* Oid of the table to do conversion on */
	Oid relationId;

	/*
	 * Options to do conversions on the table
	 * distributionColumn is the name of the new distribution column,
	 * shardCountIsNull is if the shardCount variable is not given
	 * shardCount is the new shard count,
	 * colocateWith is the name of the table to colocate with, 'none', or
	 * 'default'
	 * accessMethod is the name of the new accessMethod for the table
	 */
	char *distributionColumn;
	bool shardCountIsNull;
	int shardCount;
	char *colocateWith;
	char *accessMethod;

	/*
	 * cascadeToColocated determines whether the shardCount and
	 * colocateWith will be cascaded to the currently colocated tables
	 */
	CascadeToColocatedOption cascadeToColocated;

	/*
	 * cascadeViaForeignKeys determines if the conversion operation
	 * will be cascaded to the graph connected with foreign keys
	 * to the table
	 */
	bool cascadeViaForeignKeys;


	/* schema of the table */
	char *schemaName;
	Oid schemaId;

	/* name of the table */
	char *relationName;

	/* new relation oid after the conversion */
	Oid newRelationId;

	/* temporary name for intermediate table */
	char *tempName;

	/*hash that is appended to the name to create tempName */
	uint32 hashOfName;

	/* shard count of the table before conversion */
	int originalShardCount;

	/* list of the table oids of tables colocated with the table before conversion */
	List *colocatedTableList;

	/* new distribution key, if distributionColumn variable is given */
	Var *distributionKey;

	/* distribution key of the table before conversion */
	Var *originalDistributionKey;

	/* access method name of the table before conversion */
	char *originalAccessMethod;

	/*
	 * The function that will be used for the conversion
	 * Must comply with conversionType
	 * UNDISTRIBUTE_TABLE -> UndistributeTable
	 * ALTER_DISTRIBUTED_TABLE -> AlterDistributedTable
	 * ALTER_TABLE_SET_ACCESS_METHOD -> AlterTableSetAccessMethod
	 */
	TableConversionFunction function;

	/*
	 * suppressNoticeMessages determines if we want to suppress NOTICE
	 * messages that we explicitly issue
	 */
	bool suppressNoticeMessages;
} TableConversionState;


static TableConversionReturn * AlterTableSetAccessMethod(
	TableConversionParameters *params);
static TableConversionReturn * ConvertTable(TableConversionState *con);
static void DropIndexesNotSupportedByColumnar(Oid relationId,
											  bool suppressNoticeMessages);
static char * GetIndexAccessMethodName(Oid indexId);
static void DropConstraintRestrict(Oid relationId, Oid constraintId);
static void DropIndexRestrict(Oid indexId);
static void EnsureTableNotReferencing(Oid relationId, char conversionType);
static void EnsureTableNotReferenced(Oid relationId, char conversionType);
static void EnsureTableNotForeign(Oid relationId);
static void EnsureTableNotPartition(Oid relationId);
static TableConversionState * CreateTableConversion(TableConversionParameters *params);
static void ReplaceTable(Oid sourceId, Oid targetId, List *justBeforeDropCommands,
						 bool suppressNoticeMessages);
static bool HasAnyGeneratedStoredColumns(Oid relationId);
static List * GetNonGeneratedStoredColumnNameList(Oid relationId);
static void ErrorIfMatViewSizeExceedsTheLimit(Oid matViewOid);
static char * CreateMaterializedViewDDLCommand(Oid matViewOid);
static char * GetAccessMethodForMatViewIfExists(Oid viewOid);
static bool WillRecreateForeignKeyToReferenceTable(Oid relationId,
												   CascadeToColocatedOption cascadeOption);
static void ErrorIfUnsupportedCascadeObjects(Oid relationId);
static bool DoesCascadeDropUnsupportedObject(Oid classId, Oid id, HTAB *nodeMap);

PG_FUNCTION_INFO_V1(alter_table_set_access_method);

/* global variable keeping track of whether we are in a table type conversion function */
bool InTableTypeConversionFunctionCall = false;

/* controlled by GUC, in MB */
int MaxMatViewSizeToAutoRecreate = 1024;

/*
 * alter_table_set_access_method gets a distributed table and an access
 * method and changes table's access method into that.
 */
Datum
alter_table_set_access_method(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);

	text *accessMethodText = PG_GETARG_TEXT_P(1);
	char *accessMethod = text_to_cstring(accessMethodText);

	TableConversionParameters params = {
		.relationId = relationId,
		.accessMethod = accessMethod
	};

	AlterTableSetAccessMethod(&params);

	PG_RETURN_VOID();
}


/*
 * AlterTableSetAccessMethod changes the access method of the given table. It uses
 * ConvertTable function to create a new table with the access method and move everything
 * to that table.
 *
 * The local and references tables, tables with references, partition tables and foreign
 * tables are not supported. The function gives errors in these cases.
 */
TableConversionReturn *
AlterTableSetAccessMethod(TableConversionParameters *params)
{
	EnsureRelationExists(params->relationId);
	EnsureTableOwner(params->relationId);

	if (IsCitusTable(params->relationId))
	{
		EnsureCoordinator();
	}

	EnsureTableNotReferencing(params->relationId, ALTER_TABLE_SET_ACCESS_METHOD);
	EnsureTableNotReferenced(params->relationId, ALTER_TABLE_SET_ACCESS_METHOD);
	EnsureTableNotForeign(params->relationId);
	if (IsCitusTableType(params->relationId, DISTRIBUTED_TABLE))
	{
		EnsureHashDistributedTable(params->relationId);
	}

	if (PartitionedTable(params->relationId))
	{
		ereport(ERROR, (errmsg("you cannot alter access method of a partitioned table")));
	}

	if (PartitionTable(params->relationId) &&
		IsCitusTableType(params->relationId, DISTRIBUTED_TABLE))
	{
		Oid parentRelationId = PartitionParentOid(params->relationId);
		if (HasForeignKeyToReferenceTable(parentRelationId))
		{
			ereport(DEBUG1, (errmsg("setting multi shard modify mode to sequential")));
			SetLocalMultiShardModifyModeToSequential();
		}
	}

	ErrorIfUnsupportedCascadeObjects(params->relationId);

	params->conversionType = ALTER_TABLE_SET_ACCESS_METHOD;
	params->shardCountIsNull = true;
	TableConversionState *con = CreateTableConversion(params);

	if (strcmp(con->originalAccessMethod, con->accessMethod) == 0)
	{
		ereport(ERROR, (errmsg("the access method of %s is already %s",
							   generate_qualified_relation_name(con->relationId),
							   con->accessMethod)));
	}

	return ConvertTable(con);
}


/*
 * ConvertTable is used for converting a table into a new table with different properties.
 * The conversion is done by creating a new table, moving everything to the new table and
 * dropping the old one. So the oid of the table is not preserved.
 *
 * The new table will have the same name, columns and rows. It will also have partitions,
 * views, sequences of the old table. Finally it will have everything created by
 * GetPostLoadTableCreationCommands function, which include indexes. These will be
 * re-created during conversion, so their oids are not preserved either (except for
 * sequences). However, their names are preserved.
 *
 * The dropping of old table is done with CASCADE. Anything not mentioned here will
 * be dropped.
 *
 * The function returns a TableConversionReturn object that can stores variables that
 * can be used at the caller operations.
 *
 * To be able to provide more meaningful messages while converting a table type,
 * Citus keeps InTableTypeConversionFunctionCall flag. Don't forget to set it properly
 * in case you add a new way to return from this function.
 */
TableConversionReturn *
ConvertTable(TableConversionState *con)
{
	InTableTypeConversionFunctionCall = true;

	char *newAccessMethod = con->accessMethod ? con->accessMethod :
							con->originalAccessMethod;
	IncludeSequenceDefaults includeSequenceDefaults = NEXTVAL_SEQUENCE_DEFAULTS;
	List *preLoadCommands = GetPreLoadTableCreationCommands(con->relationId,
															includeSequenceDefaults,
															newAccessMethod);

	if (con->accessMethod && strcmp(con->accessMethod, "columnar") == 0)
	{
		DropIndexesNotSupportedByColumnar(con->relationId,
										  con->suppressNoticeMessages);
	}

	/*
	 * Since we already dropped unsupported indexes, we can safely pass
	 * includeIndexes to be true.
	 */
	bool includeIndexes = true;
	bool includeReplicaIdentity = true;
	List *postLoadCommands = GetPostLoadTableCreationCommands(con->relationId,
															  includeIndexes,
															  includeReplicaIdentity);
	List *justBeforeDropCommands = NIL;
	List *attachPartitionCommands = NIL;

	postLoadCommands =
		list_concat(postLoadCommands,
					GetViewCreationTableDDLCommandsOfTable(con->relationId));

	List *foreignKeyCommands = NIL;

	bool isPartitionTable = false;
	char *attachToParentCommand = NULL;
	if (PartitionTable(con->relationId))
	{
		isPartitionTable = true;
		char *detachFromParentCommand = GenerateDetachPartitionCommand(con->relationId);
		attachToParentCommand = GenerateAlterTableAttachPartitionCommand(con->relationId);

		justBeforeDropCommands = lappend(justBeforeDropCommands, detachFromParentCommand);
	}

	if (PartitionedTable(con->relationId))
	{
		if (!con->suppressNoticeMessages)
		{
			ereport(NOTICE, (errmsg("converting the partitions of %s",
									quote_qualified_identifier(con->schemaName,
															   con->relationName))));
		}

		List *partitionList = PartitionList(con->relationId);

		Oid partitionRelationId = InvalidOid;
		foreach_oid(partitionRelationId, partitionList)
		{
			char *tableQualifiedName = generate_qualified_relation_name(
				partitionRelationId);
			char *detachPartitionCommand = GenerateDetachPartitionCommand(
				partitionRelationId);
			char *attachPartitionCommand = GenerateAlterTableAttachPartitionCommand(
				partitionRelationId);

			/*
			 * We first detach the partitions to be able to convert them separately.
			 * After this they are no longer partitions, so they will not be caught by
			 * the checks.
			 */
			ExecuteQueryViaSPI(detachPartitionCommand, SPI_OK_UTILITY);
			attachPartitionCommands = lappend(attachPartitionCommands,
											  attachPartitionCommand);

			CascadeToColocatedOption cascadeOption = CASCADE_TO_COLOCATED_NO;
			if (con->cascadeToColocated == CASCADE_TO_COLOCATED_YES ||
				con->cascadeToColocated == CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED)
			{
				cascadeOption = CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED;
			}

			TableConversionParameters partitionParam = {
				.relationId = partitionRelationId,
				.distributionColumn = con->distributionColumn,
				.shardCountIsNull = con->shardCountIsNull,
				.shardCount = con->shardCount,
				.cascadeToColocated = cascadeOption,
				.colocateWith = con->colocateWith,
				.suppressNoticeMessages = con->suppressNoticeMessages,

				/*
				 * Even if we called UndistributeTable with cascade option, we
				 * shouldn't cascade via foreign keys on partitions. Otherwise,
				 * we might try to undistribute partitions of other tables in
				 * our foreign key subgraph more than once.
				 */
				.cascadeViaForeignKeys = false
			};

			TableConversionReturn *partitionReturn = con->function(&partitionParam);
			if (cascadeOption == CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED)
			{
				foreignKeyCommands = list_concat(foreignKeyCommands,
												 partitionReturn->foreignKeyCommands);
			}


			/*
			 * If we are altering a partitioned distributed table by
			 * colocateWith:none, we override con->colocationWith parameter
			 * with the first newly created partition table to share the
			 * same colocation group for rest of partitions and partitioned
			 * table.
			 */
			if (con->colocateWith != NULL && IsColocateWithNone(con->colocateWith))
			{
				con->colocateWith = tableQualifiedName;
			}
		}
	}

	if (!con->suppressNoticeMessages)
	{
		ereport(NOTICE, (errmsg("creating a new table for %s",
								quote_qualified_identifier(con->schemaName,
														   con->relationName))));
	}

	TableDDLCommand *tableCreationCommand = NULL;
	foreach_ptr(tableCreationCommand, preLoadCommands)
	{
		Assert(CitusIsA(tableCreationCommand, TableDDLCommand));

		char *tableCreationSql = GetTableDDLCommand(tableCreationCommand);
		Node *parseTree = ParseTreeNode(tableCreationSql);

		RelayEventExtendNames(parseTree, con->schemaName, con->hashOfName);
		ProcessUtilityParseTree(parseTree, tableCreationSql, PROCESS_UTILITY_QUERY,
								NULL, None_Receiver, NULL);
	}

	/* set columnar options */
	if (con->accessMethod == NULL && con->originalAccessMethod &&
		strcmp(con->originalAccessMethod, "columnar") == 0)
	{
		ColumnarOptions options = { 0 };
		extern_ReadColumnarOptions(con->relationId, &options);

		ColumnarTableDDLContext *context = (ColumnarTableDDLContext *) palloc0(
			sizeof(ColumnarTableDDLContext));

		/* build the context */
		context->schemaName = con->schemaName;
		context->relationName = con->relationName;
		context->options = options;

		char *columnarOptionsSql = GetShardedTableDDLCommandColumnar(con->hashOfName,
																	 context);

		ExecuteQueryViaSPI(columnarOptionsSql, SPI_OK_UTILITY);
	}

	con->newRelationId = get_relname_relid(con->tempName, con->schemaId);

	ReplaceTable(con->relationId, con->newRelationId, justBeforeDropCommands,
				 con->suppressNoticeMessages);

	TableDDLCommand *tableConstructionCommand = NULL;
	foreach_ptr(tableConstructionCommand, postLoadCommands)
	{
		Assert(CitusIsA(tableConstructionCommand, TableDDLCommand));
		char *tableConstructionSQL = GetTableDDLCommand(tableConstructionCommand);
		ExecuteQueryViaSPI(tableConstructionSQL, SPI_OK_UTILITY);
	}

	char *attachPartitionCommand = NULL;
	foreach_ptr(attachPartitionCommand, attachPartitionCommands)
	{
		Node *parseTree = ParseTreeNode(attachPartitionCommand);

		ProcessUtilityParseTree(parseTree, attachPartitionCommand,
								PROCESS_UTILITY_QUERY,
								NULL, None_Receiver, NULL);
	}

	if (isPartitionTable)
	{
		ExecuteQueryViaSPI(attachToParentCommand, SPI_OK_UTILITY);
	}

	/* recreate foreign keys */
	TableConversionReturn *ret = NULL;

	/* increment command counter so that next command can see the new table */
	CommandCounterIncrement();

	InTableTypeConversionFunctionCall = false;
	return ret;
}


/*
 * DropIndexesNotSupportedByColumnar is a helper function used during accces
 * method conversion to drop the indexes that are not supported by columnarAM.
 */
static void
DropIndexesNotSupportedByColumnar(Oid relationId, bool suppressNoticeMessages)
{
	Relation columnarRelation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(columnarRelation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *indexIdList = RelationGetIndexList(columnarRelation);

	/*
	 * Immediately close the relation since we might execute ALTER TABLE
	 * for that relation.
	 */
	RelationClose(columnarRelation);

	Oid indexId = InvalidOid;
	foreach_oid(indexId, indexIdList)
	{
		char *indexAmName = GetIndexAccessMethodName(indexId);
		if (extern_ColumnarSupportsIndexAM(indexAmName))
		{
			continue;
		}

		if (!suppressNoticeMessages)
		{
			ereport(NOTICE, (errmsg("unsupported access method for index %s "
									"on columnar table %s, given index and "
									"the constraint depending on the index "
									"(if any) will be dropped",
									get_rel_name(indexId),
									generate_qualified_relation_name(relationId))));
		}

		Oid constraintId = get_index_constraint(indexId);
		if (OidIsValid(constraintId))
		{
			/* index is implied by a constraint, so drop the constraint itself */
			DropConstraintRestrict(relationId, constraintId);
		}
		else
		{
			DropIndexRestrict(indexId);
		}
	}
}


/*
 * GetIndexAccessMethodName returns access method name of index with indexId.
 * If there is no such index, then errors out.
 */
static char *
GetIndexAccessMethodName(Oid indexId)
{
	/* fetch pg_class tuple of the index relation */
	HeapTuple indexTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(indexId));
	if (!HeapTupleIsValid(indexTuple))
	{
		ereport(ERROR, (errmsg("index with oid %u does not exist", indexId)));
	}

	Form_pg_class indexForm = (Form_pg_class) GETSTRUCT(indexTuple);
	Oid indexAMId = indexForm->relam;
	ReleaseSysCache(indexTuple);

	char *indexAmName = get_am_name(indexAMId);
	if (!indexAmName)
	{
		ereport(ERROR, (errmsg("access method with oid %u does not exist", indexAMId)));
	}

	return indexAmName;
}


/*
 * DropConstraintRestrict drops the constraint with constraintId by using spi.
 */
static void
DropConstraintRestrict(Oid relationId, Oid constraintId)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	char *constraintName = get_constraint_name(constraintId);
	const char *quotedConstraintName = quote_identifier(constraintName);
	StringInfo dropConstraintCommand = makeStringInfo();
	appendStringInfo(dropConstraintCommand, "ALTER TABLE %s DROP CONSTRAINT %s RESTRICT;",
					 qualifiedRelationName, quotedConstraintName);
	ExecuteQueryViaSPI(dropConstraintCommand->data, SPI_OK_UTILITY);
}


/*
 * DropIndexRestrict drops the index with indexId by using spi.
 */
static void
DropIndexRestrict(Oid indexId)
{
	char *qualifiedIndexName = generate_qualified_relation_name(indexId);
	StringInfo dropIndexCommand = makeStringInfo();
	appendStringInfo(dropIndexCommand, "DROP INDEX %s RESTRICT;", qualifiedIndexName);
	ExecuteQueryViaSPI(dropIndexCommand->data, SPI_OK_UTILITY);
}


/*
 * EnsureTableNotReferencing checks if the table has a reference to another
 * table and errors if it is.
 */
void
EnsureTableNotReferencing(Oid relationId, char conversionType)
{
	if (TableReferencing(relationId))
	{
		if (conversionType == UNDISTRIBUTE_TABLE)
		{
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s has a foreign key",
								   get_rel_name(relationId)),
							errhint(UNDISTRIBUTE_TABLE_CASCADE_HINT,
									qualifiedRelationName,
									qualifiedRelationName)));
		}
		else
		{
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s has a foreign key",
								   get_rel_name(relationId))));
		}
	}
}


/*
 * EnsureTableNotReferenced checks if the table is referenced by another
 * table and errors if it is.
 */
void
EnsureTableNotReferenced(Oid relationId, char conversionType)
{
	if (TableReferenced(relationId))
	{
		if (conversionType == UNDISTRIBUTE_TABLE)
		{
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s is referenced by a foreign key",
								   get_rel_name(relationId)),
							errhint(UNDISTRIBUTE_TABLE_CASCADE_HINT,
									qualifiedRelationName,
									qualifiedRelationName)));
		}
		else
		{
			ereport(ERROR, (errmsg("cannot complete operation "
								   "because table %s is referenced by a foreign key",
								   get_rel_name(relationId))));
		}
	}
}


/*
 * EnsureTableNotForeign checks if the table is a foreign table and errors
 * if it is.
 */
void
EnsureTableNotForeign(Oid relationId)
{
	if (IsForeignTable(relationId))
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because it is a foreign table")));
	}
}


/*
 * EnsureTableNotPartition checks if the table is a partition of another
 * table and errors if it is.
 */
void
EnsureTableNotPartition(Oid relationId)
{
	if (PartitionTable(relationId))
	{
		Oid parentRelationId = PartitionParentOid(relationId);
		char *parentRelationName = get_rel_name(parentRelationId);
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because table is a partition"),
						errhint("the parent table is \"%s\"",
								parentRelationName)));
	}
}


TableConversionState *
CreateTableConversion(TableConversionParameters *params)
{
	TableConversionState *con = palloc0(sizeof(TableConversionState));

	con->conversionType = params->conversionType;
	con->relationId = params->relationId;
	con->distributionColumn = params->distributionColumn;
	con->shardCountIsNull = params->shardCountIsNull;
	con->shardCount = params->shardCount;
	con->colocateWith = params->colocateWith;
	con->accessMethod = params->accessMethod;
	con->cascadeToColocated = params->cascadeToColocated;
	con->cascadeViaForeignKeys = params->cascadeViaForeignKeys;
	con->suppressNoticeMessages = params->suppressNoticeMessages;

	Relation relation = try_relation_open(con->relationId, ExclusiveLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because no such table exists")));
	}

	TupleDesc relationDesc = RelationGetDescr(relation);
	if (RelationUsesIdentityColumns(relationDesc))
	{
		/*
		 * pg_get_tableschemadef_string doesn't know how to deparse identity
		 * columns so we cannot reflect those columns when creating table
		 * from scratch. For this reason, error out here.
		 */
		ereport(ERROR, (errmsg("cannot complete command because relation "
							   "%s has identity column",
							   generate_qualified_relation_name(con->relationId)),
						errhint("Drop the identity columns and re-try the command")));
	}
	relation_close(relation, NoLock);
	con->distributionKey =
		BuildDistributionKeyFromColumnName(con->relationId, con->distributionColumn,
										   NoLock);

	con->originalAccessMethod = NULL;
	if (!PartitionedTable(con->relationId) && !IsForeignTable(con->relationId))
	{
		HeapTuple amTuple = SearchSysCache1(AMOID, ObjectIdGetDatum(
												relation->rd_rel->relam));
		if (!HeapTupleIsValid(amTuple))
		{
			ereport(ERROR, (errmsg("cache lookup failed for access method %d",
								   relation->rd_rel->relam)));
		}
		Form_pg_am amForm = (Form_pg_am) GETSTRUCT(amTuple);
		con->originalAccessMethod = NameStr(amForm->amname);
		ReleaseSysCache(amTuple);
	}


	con->colocatedTableList = NIL;
	if (IsCitusTableType(con->relationId, DISTRIBUTED_TABLE))
	{
		con->originalDistributionKey = DistPartitionKey(con->relationId);

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(con->relationId);
		con->originalShardCount = cacheEntry->shardIntervalArrayLength;

		List *colocatedTableList = ColocatedTableList(con->relationId);

		/*
		 * we will not add partition tables to the colocatedTableList
		 * since they will be handled separately.
		 */
		Oid colocatedTableId = InvalidOid;
		foreach_oid(colocatedTableId, colocatedTableList)
		{
			if (PartitionTable(colocatedTableId))
			{
				continue;
			}
			con->colocatedTableList = lappend_oid(con->colocatedTableList,
												  colocatedTableId);
		}

		/* sort the oids to avoid deadlock */
		con->colocatedTableList = SortList(con->colocatedTableList, CompareOids);
	}

	/* find relation and schema names */
	con->relationName = get_rel_name(con->relationId);
	con->schemaId = get_rel_namespace(con->relationId);
	con->schemaName = get_namespace_name(con->schemaId);

	/* calculate a temp name for the new table */
	con->tempName = pstrdup(con->relationName);
	con->hashOfName = hash_any((unsigned char *) con->tempName, strlen(con->tempName));
	AppendShardIdToName(&con->tempName, con->hashOfName);

	if (con->conversionType == UNDISTRIBUTE_TABLE)
	{
		con->function = &UndistributeTable;
	}
	else if (con->conversionType == ALTER_DISTRIBUTED_TABLE)
	{
		con->function = &AlterDistributedTable;
	}
	else if (con->conversionType == ALTER_TABLE_SET_ACCESS_METHOD)
	{
		con->function = &AlterTableSetAccessMethod;
	}

	return con;
}


/*
 * ErrorIfUnsupportedCascadeObjects gets oid of a relation, finds the objects
 * that dropping this relation cascades into and errors if there are any extensions
 * that would be dropped.
 */
static void
ErrorIfUnsupportedCascadeObjects(Oid relationId)
{
	HTAB *nodeMap = CreateSimpleHashSetWithName(Oid, "object dependency map (oid)");

	bool unsupportedObjectInDepGraph =
		DoesCascadeDropUnsupportedObject(RelationRelationId, relationId, nodeMap);

	if (unsupportedObjectInDepGraph)
	{
		ereport(ERROR, (errmsg("cannot alter table because an extension depends on it")));
	}
}


/*
 * DoesCascadeDropUnsupportedObject walks through the objects that depend on the
 * object with object id and returns true if it finds any unsupported objects.
 *
 * This function only checks extensions as unsupported objects.
 *
 * Extension dependency is different than the rest. If an object depends on an extension
 * dropping the object would drop the extension too.
 * So we check with IsAnyObjectAddressOwnedByExtension function.
 */
static bool
DoesCascadeDropUnsupportedObject(Oid classId, Oid objectId, HTAB *nodeMap)
{
	bool found = false;
	hash_search(nodeMap, &objectId, HASH_ENTER, &found);

	if (found)
	{
		return false;
	}

	ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*objectAddress, classId, objectId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(objectAddress), NULL))
	{
		return true;
	}

	Oid targetObjectClassId = classId;
	Oid targetObjectId = objectId;
	List *dependencyTupleList = GetPgDependTuplesForDependingObjects(targetObjectClassId,
																	 targetObjectId);

	HeapTuple depTup = NULL;
	foreach_ptr(depTup, dependencyTupleList)
	{
		Form_pg_depend pg_depend = (Form_pg_depend) GETSTRUCT(depTup);

		Oid dependingOid = InvalidOid;
		Oid dependingClassId = InvalidOid;

		if (pg_depend->classid == RewriteRelationId)
		{
			dependingOid = GetDependingView(pg_depend);
			dependingClassId = RelationRelationId;
		}
		else
		{
			dependingOid = pg_depend->objid;
			dependingClassId = pg_depend->classid;
		}

		if (DoesCascadeDropUnsupportedObject(dependingClassId, dependingOid, nodeMap))
		{
			return true;
		}
	}
	return false;
}


/*
 * GetViewCreationCommandsOfTable takes a table oid generates the CREATE VIEW
 * commands for views that depend to the given table. This includes the views
 * that recursively depend on the table too.
 */
List *
GetViewCreationCommandsOfTable(Oid relationId)
{
	List *views = GetDependingViews(relationId);

	List *commands = NIL;

	Oid viewOid = InvalidOid;
	foreach_oid(viewOid, views)
	{
		StringInfo query = makeStringInfo();

		/* See comments on CreateMaterializedViewDDLCommand for its limitations */
		if (get_rel_relkind(viewOid) == RELKIND_MATVIEW)
		{
			ErrorIfMatViewSizeExceedsTheLimit(viewOid);

			char *matViewCreateCommands = CreateMaterializedViewDDLCommand(viewOid);
			appendStringInfoString(query, matViewCreateCommands);
		}
		else
		{
			char *viewCreateCommand = CreateViewDDLCommand(viewOid);
			appendStringInfoString(query, viewCreateCommand);
		}

		char *alterViewCommmand = AlterViewOwnerCommand(viewOid);
		appendStringInfoString(query, alterViewCommmand);

		commands = lappend(commands, query->data);
	}

	return commands;
}


/*
 * GetViewCreationTableDDLCommandsOfTable is the same as GetViewCreationCommandsOfTable,
 * but the returned list includes objects of TableDDLCommand's, not strings.
 */
List *
GetViewCreationTableDDLCommandsOfTable(Oid relationId)
{
	List *commands = GetViewCreationCommandsOfTable(relationId);
	List *tableDDLCommands = NIL;

	char *command = NULL;
	foreach_ptr(command, commands)
	{
		tableDDLCommands = lappend(tableDDLCommands, makeTableDDLCommandString(command));
	}

	return tableDDLCommands;
}


/*
 * ErrorIfMatViewSizeExceedsTheLimit takes the oid of a materialized view and errors
 * out if the size of the matview exceeds the limit set by the GUC
 * citus.max_matview_size_to_auto_recreate.
 */
static void
ErrorIfMatViewSizeExceedsTheLimit(Oid matViewOid)
{
	if (MaxMatViewSizeToAutoRecreate >= 0)
	{
		/* if it's below 0, it means the user has removed the limit */
		Datum relSizeDatum = DirectFunctionCall1(pg_total_relation_size,
												 ObjectIdGetDatum(matViewOid));
		uint64 matViewSize = DatumGetInt64(relSizeDatum);

		/* convert from MB to bytes */
		uint64 limitSizeInBytes = MaxMatViewSizeToAutoRecreate * 1024L * 1024L;

		if (matViewSize > limitSizeInBytes)
		{
			ereport(ERROR, (errmsg("size of the materialized view %s exceeds "
								   "citus.max_matview_size_to_auto_recreate "
								   "(currently %d MB)", get_rel_name(matViewOid),
								   MaxMatViewSizeToAutoRecreate),
							errdetail("Citus restricts automatically recreating "
									  "materialized views that are larger than the "
									  "limit, because it could take too long."),
							errhint(
								"Consider increasing the size limit by setting "
								"citus.max_matview_size_to_auto_recreate; "
								"or you can remove the limit by setting it to -1")));
		}
	}
}


/*
 * CreateMaterializedViewDDLCommand creates the command to create materialized view.
 * Note that this function doesn't support
 * - Aliases
 * - Storage parameters
 * - Tablespace
 * - WITH [NO] DATA
 * options for the given materialized view. Parser functions for materialized views
 * should be added to handle them.
 *
 * Related issue: https://github.com/citusdata/citus/issues/5968
 */
static char *
CreateMaterializedViewDDLCommand(Oid matViewOid)
{
	StringInfo query = makeStringInfo();

	char *viewName = get_rel_name(matViewOid);
	char *schemaName = get_namespace_name(get_rel_namespace(matViewOid));
	char *qualifiedViewName = quote_qualified_identifier(schemaName, viewName);

	/* here we need to get the access method of the view to recreate it */
	char *accessMethodName = GetAccessMethodForMatViewIfExists(matViewOid);

	appendStringInfo(query, "CREATE MATERIALIZED VIEW %s ", qualifiedViewName);

	if (accessMethodName)
	{
		appendStringInfo(query, "USING %s ", accessMethodName);
	}

	/*
	 * Set search_path to NIL so that all objects outside of pg_catalog will be
	 * schema-prefixed.
	 */
	OverrideSearchPath *overridePath = GetOverrideSearchPath(CurrentMemoryContext);
	overridePath->schemas = NIL;
	overridePath->addCatalog = true;
	PushOverrideSearchPath(overridePath);

	/*
	 * Push the transaction snapshot to be able to get vief definition with pg_get_viewdef
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	Datum viewDefinitionDatum = DirectFunctionCall1(pg_get_viewdef,
													ObjectIdGetDatum(matViewOid));
	char *viewDefinition = TextDatumGetCString(viewDefinitionDatum);

	PopActiveSnapshot();
	PopOverrideSearchPath();

	appendStringInfo(query, "AS %s", viewDefinition);

	return query->data;
}


/*
 * ReplaceTable replaces the source table with the target table.
 * It moves all the rows of the source table to target table with INSERT SELECT.
 * Changes the dependencies of the sequences owned by source table to target table.
 * Then drops the source table and renames the target table to source tables name.
 *
 * Source and target tables need to be in the same schema and have the same columns.
 */
void
ReplaceTable(Oid sourceId, Oid targetId, List *justBeforeDropCommands,
			 bool suppressNoticeMessages)
{
	char *sourceName = get_rel_name(sourceId);
	char *targetName = get_rel_name(targetId);
	Oid schemaId = get_rel_namespace(sourceId);
	char *schemaName = get_namespace_name(schemaId);

	StringInfo query = makeStringInfo();

	if (!PartitionedTable(sourceId) && !IsForeignTable(sourceId))
	{
		if (!suppressNoticeMessages)
		{
			ereport(NOTICE, (errmsg("moving the data of %s",
									quote_qualified_identifier(schemaName, sourceName))));
		}

		if (!HasAnyGeneratedStoredColumns(sourceId))
		{
			/*
			 * Relation has no GENERATED STORED columns, copy the table via plain
			 * "INSERT INTO .. SELECT *"".
			 */
			appendStringInfo(query, "INSERT INTO %s SELECT * FROM %s",
							 quote_qualified_identifier(schemaName, targetName),
							 quote_qualified_identifier(schemaName, sourceName));
		}
		else
		{
			/*
			 * Skip columns having GENERATED ALWAYS AS (...) STORED expressions
			 * since Postgres doesn't allow inserting into such columns.
			 * This is not bad since Postgres would already generate such columns.
			 * Note that here we intentionally don't skip columns having DEFAULT
			 * expressions since user might have inserted non-default values.
			 */
			List *nonStoredColumnNameList = GetNonGeneratedStoredColumnNameList(sourceId);
			char *insertColumnString = StringJoin(nonStoredColumnNameList, ',');
			appendStringInfo(query, "INSERT INTO %s (%s) SELECT %s FROM %s",
							 quote_qualified_identifier(schemaName, targetName),
							 insertColumnString, insertColumnString,
							 quote_qualified_identifier(schemaName, sourceName));
		}

		ExecuteQueryViaSPI(query->data, SPI_OK_INSERT);
	}

	List *ownedSequences = getOwnedSequences(sourceId);
	Oid sequenceOid = InvalidOid;
	foreach_oid(sequenceOid, ownedSequences)
	{
		changeDependencyFor(RelationRelationId, sequenceOid,
							RelationRelationId, sourceId, targetId);

		/*
		 * Skip if we cannot sync metadata for target table.
		 * Checking only for the target table is sufficient since we will
		 * anyway drop the source table even if it was a Citus table that
		 * has metadata on MX workers.
		 */
		if (ShouldSyncTableMetadata(targetId))
		{
			Oid sequenceSchemaOid = get_rel_namespace(sequenceOid);
			char *sequenceSchemaName = get_namespace_name(sequenceSchemaOid);
			char *sequenceName = get_rel_name(sequenceOid);
			char *workerChangeSequenceDependencyCommand =
				CreateWorkerChangeSequenceDependencyCommand(sequenceSchemaName,
															sequenceName,
															schemaName, sourceName,
															schemaName, targetName);
			SendCommandToWorkersWithMetadata(workerChangeSequenceDependencyCommand);
		}
		else if (ShouldSyncTableMetadata(sourceId))
		{
			char *qualifiedTableName = quote_qualified_identifier(schemaName, sourceName);

			/*
			 * We are converting a citus local table to a distributed/reference table,
			 * so we should prevent dropping the sequence on the table. Otherwise, we'd
			 * lose track of the previous changes in the sequence.
			 */
			StringInfo command = makeStringInfo();

			appendStringInfo(command,
							 "SELECT pg_catalog.worker_drop_sequence_dependency(%s);",
							 quote_literal_cstr(qualifiedTableName));

			SendCommandToWorkersWithMetadata(command->data);
		}
	}

	char *justBeforeDropCommand = NULL;
	foreach_ptr(justBeforeDropCommand, justBeforeDropCommands)
	{
		ExecuteQueryViaSPI(justBeforeDropCommand, SPI_OK_UTILITY);
	}

	if (!suppressNoticeMessages)
	{
		ereport(NOTICE, (errmsg("dropping the old %s",
								quote_qualified_identifier(schemaName, sourceName))));
	}

	resetStringInfo(query);
	appendStringInfo(query, "DROP %sTABLE %s CASCADE",
					 IsForeignTable(sourceId) ? "FOREIGN " : "",
					 quote_qualified_identifier(schemaName, sourceName));
	ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);

	if (!suppressNoticeMessages)
	{
		ereport(NOTICE, (errmsg("renaming the new table to %s",
								quote_qualified_identifier(schemaName, sourceName))));
	}

	resetStringInfo(query);
	appendStringInfo(query, "ALTER TABLE %s RENAME TO %s",
					 quote_qualified_identifier(schemaName, targetName),
					 quote_identifier(sourceName));
	ExecuteQueryViaSPI(query->data, SPI_OK_UTILITY);
}


/*
 * HasAnyGeneratedStoredColumns decides if relation has any columns that we
 * might need to copy the data of when replacing table.
 */
static bool
HasAnyGeneratedStoredColumns(Oid relationId)
{
	return list_length(GetNonGeneratedStoredColumnNameList(relationId)) > 0;
}


/*
 * GetNonGeneratedStoredColumnNameList returns a list of column names for
 * columns not having GENERATED ALWAYS AS (...) STORED expressions.
 */
static List *
GetNonGeneratedStoredColumnNameList(Oid relationId)
{
	List *nonStoredColumnNameList = NIL;

	Relation relation = relation_open(relationId, AccessShareLock);
	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		Form_pg_attribute currentColumn = TupleDescAttr(tupleDescriptor, columnIndex);
		if (currentColumn->attisdropped)
		{
			/* skip dropped columns */
			continue;
		}

		if (currentColumn->attgenerated == ATTRIBUTE_GENERATED_STORED)
		{
			continue;
		}

		const char *quotedColumnName = quote_identifier(NameStr(currentColumn->attname));
		nonStoredColumnNameList = lappend(nonStoredColumnNameList,
										  pstrdup(quotedColumnName));
	}

	relation_close(relation, NoLock);

	return nonStoredColumnNameList;
}


/*
 * GetAccessMethodForMatViewIfExists returns if there's an access method
 * set to the view with the given oid. Returns NULL otherwise.
 */
static char *
GetAccessMethodForMatViewIfExists(Oid viewOid)
{
	char *accessMethodName = NULL;
	Relation relation = try_relation_open(viewOid, AccessShareLock);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("cannot complete operation "
							   "because no such view exists")));
	}

	Oid accessMethodOid = relation->rd_rel->relam;
	if (OidIsValid(accessMethodOid))
	{
		accessMethodName = get_am_name(accessMethodOid);
	}
	relation_close(relation, NoLock);

	return accessMethodName;
}


/*
 * WillRecreateForeignKeyToReferenceTable checks if the table of relationId has any foreign
 * key to a reference table, if conversion will be cascaded to colocated table this function
 * also checks if any of the colocated tables have a foreign key to a reference table too
 */
bool
WillRecreateForeignKeyToReferenceTable(Oid relationId,
									   CascadeToColocatedOption cascadeOption)
{
	if (cascadeOption == CASCADE_TO_COLOCATED_NO ||
		cascadeOption == CASCADE_TO_COLOCATED_UNSPECIFIED)
	{
		return HasForeignKeyToReferenceTable(relationId);
	}
	else if (cascadeOption == CASCADE_TO_COLOCATED_YES)
	{
		List *colocatedTableList = ColocatedTableList(relationId);
		Oid colocatedTableOid = InvalidOid;
		foreach_oid(colocatedTableOid, colocatedTableList)
		{
			if (HasForeignKeyToReferenceTable(colocatedTableOid))
			{
				return true;
			}
		}
	}
	return false;
}


/*
 * ExecuteQueryViaSPI connects to SPI, executes the query and checks if it
 * returned the OK value and finishes the SPI connection
 */
void
ExecuteQueryViaSPI(char *query, int SPIOK)
{
	int spiResult = SPI_connect();
	if (spiResult != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	spiResult = SPI_execute(query, false, 0);
	if (spiResult != SPIOK)
	{
		ereport(ERROR, (errmsg("could not run SPI query")));
	}

	spiResult = SPI_finish();
	if (spiResult != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not finish SPI connection")));
	}
}


/*
 * ExecuteAndLogQueryViaSPI is a wrapper around ExecuteQueryViaSPI, that logs
 * the query to be executed, with the given log level.
 */
void
ExecuteAndLogQueryViaSPI(char *query, int SPIOK, int logLevel)
{
	ereport(logLevel, (errmsg("executing \"%s\"", query)));

	ExecuteQueryViaSPI(query, SPIOK);
}
