package main

import (
	fenixSyncShared "github.com/jlambert68/FenixSyncShared"
	"github.com/sirupsen/logrus"
)

// Used for only process cleanup once
var cleanupProcessed = false

func cleanup() {

	if cleanupProcessed == false {

		cleanupProcessed = true

		// Cleanup before close down application
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{}).Info("Clean up and shut down servers")

		// Stop Backend gRPC Server
		fenixClientTestDataSyncServerObject.StopGrpcServer()

		//log.Println("Close DB_session: %v", DB_session)
		//DB_session.Close()
	}
}

func FenixClientServerMain() {

	// Connect to CloudDB
	fenixSyncShared.ConnectToDB()

	// Set up BackendObject
	fenixClientTestDataSyncServerObject = &fenixClientTestDataSyncServerObject_struct{
		fenixClientTestDataSyncServer_TestDataClientUuid: fenixSyncShared.MustGetEnvironmentVariable("TestDataClientUuid"),
		fenixClientTestDataSyncServer_DomainUuid:         fenixSyncShared.MustGetEnvironmentVariable("TestDomainUuid"),
		fenixClientTestDataSyncServer_DomainName:         fenixSyncShared.MustGetEnvironmentVariable("TestDomainName"),
		merkleFilterPath:                                 fenixSyncShared.MustGetEnvironmentVariable("MerkleFilterPath"), //TODO Remove all references to HARDCODED merkleFilterPath
	}

	// Init logger
	fenixClientTestDataSyncServerObject.InitLogger("")

	// TODO Endast för Test
	fenixClientTestDataSyncServerObject.loadAllTestDataRowItemsForClientFromCloudDB(&cloudDBExposedTestDataRowItems)

	// Clean up when leaving. Is placed after logger because shutdown logs information
	defer cleanup()

	// Start Backend gRPC-server
	fenixClientTestDataSyncServerObject.InitGrpcServer()

}
