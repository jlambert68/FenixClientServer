package main

import (
	"context"
	fenixSyncShared "github.com/jlambert68/FenixSyncShared"
	"github.com/sirupsen/logrus"
	"time"
)

// ****************************************************************************************************************
// Load data from CloudDB into memory structures, to speed up stuff
//
// All TestDataRowItems in CloudDB
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) loadAllTestDataRowItemsForClientFromCloudDB(testDataRowItems *[]cloudDBTestDataRowItemCurrentStruct) (err error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"Id": "61b8b021-9568-463e-b867-ac1ddb10584d",
	}).Debug("Entering: loadAllTestDataRowItemsForClientFromCloudDB()")

	defer func() {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"Id": "78a97c41-a098-4122-88d2-01ed4b6c4844",
		}).Debug("Exiting: loadAllTestDataRowItemsForClientFromCloudDB()")
	}()

	/* Example
	SELECT *
	FROM "FenixTestDataSyncClient"."CurrentExposedTestDataForClient"

	    client_uuid               uuid      not null,
	    row_hash                  varchar   not null,
	    testdata_value_as_string  varchar   not null,
	    value_column_order        integer   not null,
	    value_row_order           integer   not null,
	    updated_timestamp         timestamp not null,
	    merkletree_leaf_node_name varchar   not null,
	    merkletree_leaf_node_path varchar   not null,
	    merkletree_leaf_node_hash varchar   not null,

	*/

	usedDBSchema := fenixSyncShared.GetDBSchemaName()

	sqlToExecute := ""
	sqlToExecute = sqlToExecute + "SELECT * "
	sqlToExecute = sqlToExecute + "FROM \"" + usedDBSchema + "\".\"CurrentExposedTestDataForClient\";"

	// Query DB
	rows, err := fenixSyncShared.DbPool.Query(context.Background(), sqlToExecute)

	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"Id":           "2f130d7e-f8aa-466f-b29d-0fb63608c1a6",
			"Error":        err,
			"sqlToExecute": sqlToExecute,
		}).Error("Something went wrong when executing SQL")

		return err
	}

	// Variables to used when extract data from result set
	var testDataRowItem cloudDBTestDataRowItemCurrentStruct
	var tempTimeStamp time.Time
	var timeStampAsString string
	timeStampLayOut := fenixSyncShared.TimeStampLayOut //"2006-01-02 15:04:05.000000" //milliseconds

	// Extract data from DB result set
	for rows.Next() {
		err := rows.Scan(
			&testDataRowItem.clientUuid,
			&testDataRowItem.rowHash,
			&testDataRowItem.testdataValueAsString,
			&testDataRowItem.valueColumnOrder,
			&testDataRowItem.valueRowOrder,
			&tempTimeStamp,
			&testDataRowItem.leafNodeName,
			&testDataRowItem.leafNodePath,
			&testDataRowItem.leafNodeHash,
		)

		if err != nil {
			return err
		}

		// Convert timestamp into string representation and add to  extracted data
		timeStampAsString = tempTimeStamp.Format(timeStampLayOut)
		testDataRowItem.updatedTimeStamp = timeStampAsString

		// Add values to the object that is pointed to by variable in function
		*testDataRowItems = append(*testDataRowItems, testDataRowItem)

	}

	// No errors occurred
	return nil

}

// All TestDataRowItems in CloudDB
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) loadClientInfoFromCloudDB(clientInfo *cloudDBClientInfoStruct) (err error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"Id": "a171c667-f871-45d5-85c4-418ab3bef138",
	}).Debug("Entering: loadClientInfoFromCloudDB()")

	defer func() {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"Id": "f874ea7a-c1c7-46a0-be9d-3e7a9599dee5",
		}).Debug("Exiting: loadClientInfoFromCloudDB()")
	}()

	/* Example
	SELECT *
	FROM "FenixTestDataSyncClient".current_testdata_merkleinfo

	    client_uuid                   uuid    not null
	    merklehash                    varchar
	    labels_hash                   varchar
	    meklehash_created_timestamp   timestamp,
	    labels_hash_created_timestamp timestamp,
	    meklehash_sent_timestamp      timestamp,
	    labels_hash_sent_timestamp    timestamp,
	    full_merkle_filter_path       varchar not null,
	    domain_name                   varchar not null,
	    domain_uuid                   uuid    not null

	*/

	usedDBSchema := fenixSyncShared.GetDBSchemaName()

	sqlToExecute := ""
	sqlToExecute = sqlToExecute + "SELECT * "
	sqlToExecute = sqlToExecute + "FROM \"" + usedDBSchema + "\".current_testdata_merkleinfo;"

	// Query DB
	rows, err := fenixSyncShared.DbPool.Query(context.Background(), sqlToExecute)

	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"Id":           "2f130d7e-f8aa-466f-b29d-0fb63608c1a6",
			"Error":        err,
			"sqlToExecute": sqlToExecute,
		}).Error("Something went wrong when executing SQL")

		return err
	}

	// Verify that only one row was found
	/*
		if rows.CommandTag().RowsAffected() != 1 {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"Id":                               "381935ab-38e0-4000-a95c-ad45aeceef16",
				"rows.CommandTag().RowsAffected()": rows.CommandTag().RowsAffected(),
			}).Fatal("Didn't find exact one row in CloudDB for ClientInfo")
		}

	*/

	// Variables to used when extract data from result set
	var meklehashCreatedTimestamp time.Time
	var labelshashCreatedTimestamp time.Time
	var meklehashSentTimestamp time.Time
	var labelshashSentTimestamp time.Time

	var timeStampAsString string
	timeStampLayOut := fenixSyncShared.TimeStampLayOut //"2006-01-02 15:04:05.000000" //milliseconds

	// Extract data from DB result set
	for rows.Next() {
		err := rows.Scan(
			&clientInfo.clientUuid,
			&clientInfo.merklehash,
			&clientInfo.labels_hash,
			&meklehashCreatedTimestamp,
			&labelshashCreatedTimestamp,
			&meklehashSentTimestamp,
			&labelshashSentTimestamp,
			&clientInfo.full_merkle_filter_path,
			&clientInfo.domain_name,
			&clientInfo.domain_uuid,
		)

		if err != nil {
			return err
		}

		// Convert timestamp into string representation and add to  extracted data
		timeStampAsString = meklehashCreatedTimestamp.Format(timeStampLayOut)
		clientInfo.meklehash_created_timestamp = timeStampAsString

		timeStampAsString = labelshashCreatedTimestamp.Format(timeStampLayOut)
		clientInfo.labels_hash_created_timestamp = timeStampAsString

		timeStampAsString = meklehashSentTimestamp.Format(timeStampLayOut)
		clientInfo.meklehash_sent_timestamp = timeStampAsString

		timeStampAsString = labelshashSentTimestamp.Format(timeStampLayOut)
		clientInfo.labels_hash_sent_timestamp = timeStampAsString

	}

	// No errors occurred
	return nil

}
