/*-------------------------------------------------------------------------
 *
 * shardsplit_logical_replication.c
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "distributed/colocation_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_split.h"
#include "distributed/listutils.h"
#include "distributed/shardsplit_logical_replication.h"
#include "distributed/resource_lock.h"
#include "utils/builtins.h"
#include "commands/dbcommands.h"

static HTAB *ShardInfoHashMapForPublications = NULL;

/* function declarations */
static void AddPublishableShardEntryInMap(uint32 targetNodeId,
										  ShardInterval *shardInterval, bool
										  isChildShardInterval);
ShardSplitSubscriberMetadata * CreateShardSplitSubscriberMetadata(Oid tableOwnerId, uint32
																  nodeId,
																  List *
																  replicationSlotInfoList);

static void CreateShardSplitPublicationForNode(MultiConnection *connection,
											   List *shardList,
											   uint32_t publicationForTargetNodeId, Oid
											   tableOwner);

static char * ShardSplitPublicationName(uint32_t nodeId, Oid ownerId);


static void DropAllShardSplitSubscriptions(MultiConnection *cleanupConnection);
static void DropAllShardSplitPublications(MultiConnection *cleanupConnection);
static void DropAllShardSplitUsers(MultiConnection *cleanupConnection);
static void DropAllReplicationSlots(List *replicationSlotInfo);


List *
ParseReplicationSlotInfoFromResult(PGresult *result)
{
	int64 rowCount = PQntuples(result);
	int64 colCount = PQnfields(result);

	List *replicationSlotInfoList = NIL;
	for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		ReplicationSlotInfo *replicationSlotInfo = (ReplicationSlotInfo *) palloc0(
			sizeof(ReplicationSlotInfo));

		char *targeNodeIdString = PQgetvalue(result, rowIndex, 0);
		replicationSlotInfo->targetNodeId = strtoul(targeNodeIdString, NULL, 10);

		/* we're using the pstrdup to copy the data into the current memory context */
		replicationSlotInfo->tableOwnerName = pstrdup(PQgetvalue(result, rowIndex, 1));

		replicationSlotInfo->slotName = pstrdup(PQgetvalue(result, rowIndex, 2));

		replicationSlotInfoList = lappend(replicationSlotInfoList, replicationSlotInfo);
	}

	return replicationSlotInfoList;
}


HTAB *
CreateShardSplitInfoMapForPublication(List *sourceColocatedShardIntervalList,
									  List *shardGroupSplitIntervalListList,
									  List *destinationWorkerNodesList)
{
	ShardInfoHashMapForPublications = SetupHashMapForShardInfo();
	ShardInterval *sourceShardIntervalToCopy = NULL;
	List *splitChildShardIntervalList = NULL;
	forboth_ptr(sourceShardIntervalToCopy, sourceColocatedShardIntervalList,
				splitChildShardIntervalList, shardGroupSplitIntervalListList)
	{
		ShardInterval *splitChildShardInterval = NULL;
		WorkerNode *destinationWorkerNode = NULL;
		forboth_ptr(splitChildShardInterval, splitChildShardIntervalList,
					destinationWorkerNode, destinationWorkerNodesList)
		{
			uint32 destinationWorkerNodeId = destinationWorkerNode->nodeId;

			/* Add split child shard interval */
			AddPublishableShardEntryInMap(destinationWorkerNodeId,
										  splitChildShardInterval,
										  true /*isChildShardInterval*/);

			/* Add parent shard interval if not already added */
			AddPublishableShardEntryInMap(destinationWorkerNodeId,
										  sourceShardIntervalToCopy,
										  false /*isChildShardInterval*/);
		}
	}

	return ShardInfoHashMapForPublications;
}


static void
AddPublishableShardEntryInMap(uint32 targetNodeId, ShardInterval *shardInterval, bool
							  isChildShardInterval)
{
	NodeShardMappingKey key;
	key.nodeId = targetNodeId;
	key.tableOwnerId = TableOwnerOid(shardInterval->relationId);

	bool found = false;
	NodeShardMappingEntry *nodeMappingEntry =
		(NodeShardMappingEntry *) hash_search(ShardInfoHashMapForPublications, &key,
											  HASH_ENTER,
											  &found);
	if (!found)
	{
		nodeMappingEntry->shardSplitInfoList = NIL;
	}

	if (isChildShardInterval)
	{
		nodeMappingEntry->shardSplitInfoList =
			lappend(nodeMappingEntry->shardSplitInfoList,
					(ShardInterval *) shardInterval);
		return;
	}

	ShardInterval *existingShardInterval = NULL;
	foreach_ptr(existingShardInterval, nodeMappingEntry->shardSplitInfoList)
	{
		if (existingShardInterval->shardId == shardInterval->shardId)
		{
			/* parent shard interval is already added hence return */
			return;
		}
	}

	/* Add parent shard Interval */
	nodeMappingEntry->shardSplitInfoList =
		lappend(nodeMappingEntry->shardSplitInfoList, (ShardInterval *) shardInterval);
}


void
LogicallyReplicateSplitShards(WorkerNode *sourceWorkerNode,
							  List *shardSplitPubSubMetadataList,
							  List *sourceColocatedShardIntervalList,
							  List *shardGroupSplitIntervalListList,
							  List *destinationWorkerNodesList)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);
	int connectionFlags = FORCE_NEW_CONNECTION;

	/* Get source node connection */
	MultiConnection *sourceConnection =
		GetNodeUserDatabaseConnection(connectionFlags, sourceWorkerNode->workerName,
									  sourceWorkerNode->workerPort,
									  superUser, databaseName);

	ClaimConnectionExclusively(sourceConnection);

	List *targetNodeConnectionList = CreateTargetNodeConnectionsForShardSplit(
		shardSplitPubSubMetadataList,
		connectionFlags,
		superUser, databaseName);

	/* create publications */
	/*CreateShardSplitPublications(sourceConnection, shardSplitPubSubMetadataList); */

	CreateShardSplitSubscriptions(targetNodeConnectionList,
								  shardSplitPubSubMetadataList,
								  sourceWorkerNode,
								  superUser,
								  databaseName);

	WaitForShardSplitRelationSubscriptionsBecomeReady(shardSplitPubSubMetadataList);

	XLogRecPtr sourcePosition = GetRemoteLogPosition(sourceConnection);
	WaitForShardSplitRelationSubscriptionsToBeCaughtUp(sourcePosition,
													   shardSplitPubSubMetadataList);

	CreateAuxiliaryStructuresForShardGroup(shardGroupSplitIntervalListList,
										   destinationWorkerNodesList);

	sourcePosition = GetRemoteLogPosition(sourceConnection);
	WaitForShardSplitRelationSubscriptionsToBeCaughtUp(sourcePosition,
													   shardSplitPubSubMetadataList);

	BlockWritesToShardList(sourceColocatedShardIntervalList);

	sourcePosition = GetRemoteLogPosition(sourceConnection);
	WaitForShardSplitRelationSubscriptionsToBeCaughtUp(sourcePosition,
													   shardSplitPubSubMetadataList);

	/*TOOD : Create foreign key constraints and handle partitioned tables*/
}


void
PrintShardSplitPubSubMetadata(ShardSplitSubscriberMetadata *shardSplitMetadata)
{
	printf("\nsameer: ShardSplitPubSbuMetadata");
	ReplicationSlotInfo *replicationInfo = shardSplitMetadata->slotInfo;
	printf("Manual Username from OID at source: %s \n", GetUserNameFromId(
			   shardSplitMetadata->tableOwnerId, false));
	printf("slotname:%s  targetNode:%u tableOwner:%s \n", replicationInfo->slotName,
		   replicationInfo->targetNodeId, replicationInfo->tableOwnerName);
	printf("\n");
}

void
CreateShardSplitSubscriptions(List *targetNodeConnectionList,
							  List *shardSplitPubSubMetadataList,
							  WorkerNode *sourceWorkerNode,
							  char *superUser,
							  char *databaseName)
{
	MultiConnection *targetConnection = NULL;
	ShardSplitSubscriberMetadata *shardSplitPubSubMetadata = NULL;
	forboth_ptr(targetConnection, targetNodeConnectionList,
				shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
	{
		uint32 publicationForNodeId = shardSplitPubSubMetadata->slotInfo->targetNodeId;
		Oid ownerId = shardSplitPubSubMetadata->tableOwnerId;
		CreateShardSubscription(targetConnection,
								sourceWorkerNode->workerName,
								sourceWorkerNode->workerPort,
								superUser,
								databaseName,
								ShardSplitPublicationName(publicationForNodeId, ownerId),
								shardSplitPubSubMetadata->slotInfo->slotName,
								ownerId);
	}
}


static void
CreateShardSplitPublicationForNode(MultiConnection *connection, List *shardList,
								   uint32_t publicationForTargetNodeId, Oid ownerId)
{
	StringInfo createPublicationCommand = makeStringInfo();
	bool prefixWithComma = false;

	appendStringInfo(createPublicationCommand, "CREATE PUBLICATION %s FOR TABLE ",
					 ShardSplitPublicationName(publicationForTargetNodeId, ownerId));

	ShardInterval *shard = NULL;
	foreach_ptr(shard, shardList)
	{
		char *shardName = ConstructQualifiedShardName(shard);

		if (prefixWithComma)
		{
			appendStringInfoString(createPublicationCommand, ",");
		}

		appendStringInfoString(createPublicationCommand, shardName);
		prefixWithComma = true;
	}

	ExecuteCriticalRemoteCommand(connection, createPublicationCommand->data);
	pfree(createPublicationCommand->data);
	pfree(createPublicationCommand);
}


static char *
ShardSplitPublicationName(uint32_t nodeId, Oid ownerId)
{
	return psprintf("%s%u_%u", SHARD_SPLIT_PUBLICATION_PREFIX, nodeId, ownerId);
}


void
WaitForShardSplitRelationSubscriptionsBecomeReady(List *shardSplitPubSubMetadataList)
{
	ShardSplitSubscriberMetadata *shardSplitPubSubMetadata = NULL;
	foreach_ptr(shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
	{
		Bitmapset *tableOwnerIds = NULL;
		tableOwnerIds = bms_add_member(tableOwnerIds,
									   shardSplitPubSubMetadata->tableOwnerId);
		WaitForRelationSubscriptionsBecomeReady(
			shardSplitPubSubMetadata->targetNodeConnection, tableOwnerIds,
			SHARD_SPLIT_SUBSCRIPTION_PREFIX);
	}
}


void
WaitForShardSplitRelationSubscriptionsToBeCaughtUp(XLogRecPtr sourcePosition,
												   List *shardSplitPubSubMetadataList)
{
	ShardSplitSubscriberMetadata *shardSplitPubSubMetadata = NULL;
	foreach_ptr(shardSplitPubSubMetadata, shardSplitPubSubMetadataList)
	{
		Bitmapset *tableOwnerIds = NULL;
		tableOwnerIds = bms_add_member(tableOwnerIds,
									   shardSplitPubSubMetadata->tableOwnerId);

		WaitForShardSubscriptionToCatchUp(shardSplitPubSubMetadata->targetNodeConnection,
										  sourcePosition,
										  tableOwnerIds,
										  SHARD_SPLIT_SUBSCRIPTION_PREFIX);
	}
}


List *
CreateTargetNodeConnectionsForShardSplit(List *shardSplitSubscribersMetadataList, int
										 connectionFlags, char *user, char *databaseName)
{
	List *targetNodeConnectionList = NIL;
	ShardSplitSubscriberMetadata *shardSplitSubscriberMetadata = NULL;
	foreach_ptr(shardSplitSubscriberMetadata, shardSplitSubscribersMetadataList)
	{
		/*TODO(saawasek):For slot equals not null */
		uint32 targetWorkerNodeId = shardSplitSubscriberMetadata->slotInfo->targetNodeId;
		WorkerNode *targetWorkerNode = FindNodeWithNodeId(targetWorkerNodeId, false);

		MultiConnection *targetConnection =
			GetNodeUserDatabaseConnection(connectionFlags, targetWorkerNode->workerName,
										  targetWorkerNode->workerPort,
										  user,
										  databaseName);
		ClaimConnectionExclusively(targetConnection);

		targetNodeConnectionList = lappend(targetNodeConnectionList, targetConnection);

		shardSplitSubscriberMetadata->targetNodeConnection = targetConnection;
	}

	return targetNodeConnectionList;
}


char *
DropExistingIfAnyAndCreateTemplateReplicationSlot(ShardInterval *shardIntervalToSplit,
												  MultiConnection *sourceConnection)
{
	StringInfo splitTemplateReplicationSlotName = makeStringInfo();
	appendStringInfo(splitTemplateReplicationSlotName,
					 "citus_split_replicationslot_for_shard_%lu",
					 shardIntervalToSplit->shardId);

	/*
	 * To ensure SPLIT is idempotent drop any existing slot from
	 * previous failed operation.
	 */
	StringInfo dropReplicationSlotCommand = makeStringInfo();
	appendStringInfo(dropReplicationSlotCommand, "SELECT pg_drop_replication_slot('%s')",
					 splitTemplateReplicationSlotName->data);

	/* The Drop command can fail so ignore the response / result and proceed anyways */
	PGresult *result = NULL;
	int response = ExecuteOptionalRemoteCommand(sourceConnection,
												dropReplicationSlotCommand->data,
												&result);

	PQclear(result);
	ForgetResults(sourceConnection);

	/*
	 * PG 13 Function: pg_create_logical_replication_slot ( slot_name name, plugin name [, temporary boolean ] )
	 * PG 14 Function: pg_create_logical_replication_slot (slot_name name, plugin name [, temporary boolean, two_phase boolean ] )
	 * Return: record ( slot_name name, lsn pg_lsn )
	 * Note: Temporary slot are only live during the session's lifetime causing them to be dropped when the session ends.
	 * In our invocation 'two_phase' support is disabled.
	 */
	StringInfo createReplicationSlotCommand = makeStringInfo();

	/* TODO(niupre): Replace pgoutput with an appropriate name (to be introduced in by saawasek's PR) */
	/*TODO(saawasek): Try creating TEMPORAL once basic flow is ready and we have a testcase*/
	appendStringInfo(createReplicationSlotCommand,
					 "CREATE_REPLICATION_SLOT %s LOGICAL citus EXPORT_SNAPSHOT;",
					 splitTemplateReplicationSlotName->data);

	response = ExecuteOptionalRemoteCommand(sourceConnection,
											createReplicationSlotCommand->data, &result);

	if (response != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
	{
		ReportResultError(sourceConnection, result, ERROR);
	}

	/*'snapshot_name' is second column where index starts from zero.
	 * We're using the pstrdup to copy the data into the current memory context */
	char *snapShotName = pstrdup(PQgetvalue(result, 0, 2 /* columIndex */));

	return snapShotName;
}


void
DropAllShardSplitLeftOvers(WorkerNode *sourceNode, HTAB *shardSplitHashMapForPubSub)
{
	char *superUser = CitusExtensionOwnerName();
	char *databaseName = get_database_name(MyDatabaseId);

	/*
	 * We open new connections to all nodes. The reason for this is that
	 * operations on subscriptions and publications cannot be run in a
	 * transaction. By forcing a new connection we make sure no transaction is
	 * active on the connection.
	 */
	int connectionFlags = FORCE_NEW_CONNECTION;

	HASH_SEQ_STATUS statusForSubscription;
	hash_seq_init(&statusForSubscription, shardSplitHashMapForPubSub);

	NodeShardMappingEntry *entry = NULL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&statusForSubscription)) !=
		   NULL)
	{
		uint32_t nodeId = entry->key.nodeId;
		WorkerNode *workerNode = FindNodeWithNodeId(nodeId, false /*missingOk*/);
		MultiConnection *cleanupConnection = GetNodeUserDatabaseConnection(
			connectionFlags, workerNode->workerName, workerNode->workerPort,
			superUser, databaseName);

		DropAllShardSplitSubscriptions(cleanupConnection);
		DropAllShardSplitUsers(cleanupConnection);

		CloseConnection(cleanupConnection);
	}

	/*Drop all shard split publications at the source*/
	MultiConnection *sourceNodeConnection = GetNodeUserDatabaseConnection(
		connectionFlags, sourceNode->workerName, sourceNode->workerPort,
		superUser, databaseName);

	DropAllShardSplitPublications(sourceNodeConnection);

	CloseConnection(sourceNodeConnection);
}


void
DropAllShardSplitSubscriptions(MultiConnection *cleanupConnection)
{
	char *query = psprintf(
		"SELECT subname FROM pg_subscription "
		"WHERE subname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_SUBSCRIPTION_PREFIX));
	List *subscriptionNameList = GetQueryResultStringList(cleanupConnection, query);
	char *subscriptionName = NULL;
	foreach_ptr(subscriptionName, subscriptionNameList)
	{
		DropShardSubscription(cleanupConnection, subscriptionName);
	}
}


static void
DropAllShardSplitPublications(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT pubname FROM pg_publication "
		"WHERE pubname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_PUBLICATION_PREFIX));
	List *publicationNameList = GetQueryResultStringList(connection, query);
	char *publicationName;
	foreach_ptr(publicationName, publicationNameList)
	{
		DropShardPublication(connection, publicationName);
	}
}


void
DropAllShardSplitUsers(MultiConnection *connection)
{
	char *query = psprintf(
		"SELECT rolname FROM pg_roles "
		"WHERE rolname LIKE %s || '%%'",
		quote_literal_cstr(SHARD_SPLIT_SUBSCRIPTION_ROLE_PREFIX));
	List *usernameList = GetQueryResultStringList(connection, query);
	char *username;
	foreach_ptr(username, usernameList)
	{
		DropShardUser(connection, username);
	}
}


void
CreateShardSplitPublications(MultiConnection *sourceConnection,
							 HTAB *shardInfoHashMapForPublication)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardInfoHashMapForPublication);

	NodeShardMappingEntry *entry = NULL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		uint32 tableOwnerId = entry->key.tableOwnerId;
		List *shardListForPublication = entry->shardSplitInfoList;

		CreateShardSplitPublicationForNode(sourceConnection,
										   shardListForPublication,
										   nodeId,
										   tableOwnerId);
	}
}


List *
PopulateShardSplitSubscriptionsMetadataList(HTAB *shardSplitInfoHashMap,
											List *replicationSlotInfoList)
{
	HASH_SEQ_STATUS status;
	hash_seq_init(&status, shardSplitInfoHashMap);

	NodeShardMappingEntry *entry = NULL;
	List *shardSplitSubscriptionMetadataList = NIL;
	while ((entry = (NodeShardMappingEntry *) hash_seq_search(&status)) != NULL)
	{
		uint32 nodeId = entry->key.nodeId;
		uint32 tableOwnerId = entry->key.tableOwnerId;
		ShardSplitSubscriberMetadata *shardSplitSubscriberMetadata =
			CreateShardSplitSubscriberMetadata(tableOwnerId, nodeId,
											   replicationSlotInfoList);

		shardSplitSubscriptionMetadataList = lappend(shardSplitSubscriptionMetadataList,
													 shardSplitSubscriberMetadata);
	}

	return shardSplitSubscriptionMetadataList;
}


ShardSplitSubscriberMetadata *
CreateShardSplitSubscriberMetadata(Oid tableOwnerId, uint32 nodeId,
								   List *replicationSlotInfoList)
{
	ShardSplitSubscriberMetadata *shardSplitSubscriberMetadata = palloc0(
		sizeof(ShardSplitSubscriberMetadata));
	shardSplitSubscriberMetadata->tableOwnerId = tableOwnerId;

	char *tableOwnerName = GetUserNameFromId(tableOwnerId, false);
	ReplicationSlotInfo *replicationSlotInfo = NULL;
	foreach_ptr(replicationSlotInfo, replicationSlotInfoList)
	{
		if (nodeId == replicationSlotInfo->targetNodeId &&
			strcmp(tableOwnerName, replicationSlotInfo->tableOwnerName) == 0)
		{
			shardSplitSubscriberMetadata->slotInfo = replicationSlotInfo;
			break;
		}
	}

	PrintShardSplitPubSubMetadata(shardSplitSubscriberMetadata);

	return shardSplitSubscriberMetadata;
}


/*TODO(saawasek): Remove existing slots before creating newer ones */

/* extern void CreateReplicationSlots(MultiConnection *sourceNodeConnection, List * shardSplitSubscriberMetadataList); */
void
CreateReplicationSlots(MultiConnection *sourceNodeConnection,
					   List *shardSplitSubscriberMetadataList)
{
	ShardSplitSubscriberMetadata *subscriberMetadata = NULL;
	foreach_ptr(subscriberMetadata, shardSplitSubscriberMetadataList)
	{
		char *slotName = subscriberMetadata->slotInfo->slotName;

		StringInfo createReplicationSlotCommand = makeStringInfo();

		/* TODO(niupre): Replace pgoutput with an appropriate name (to e introduced in by saawasek's PR) */
		appendStringInfo(createReplicationSlotCommand,
						 "SELECT * FROM  pg_create_logical_replication_slot('%s','citus', false)",
						 slotName);

		PGresult *result = NULL;
		int response = ExecuteOptionalRemoteCommand(sourceNodeConnection,
													createReplicationSlotCommand->data,
													&result);
		if (response != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
		{
			ReportResultError(sourceNodeConnection, result, ERROR);
		}

		PQclear(result);
		ForgetResults(sourceNodeConnection);
	}
}