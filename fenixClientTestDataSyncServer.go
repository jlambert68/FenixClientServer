package main

import (
	fenixSyncShared "github.com/jlambert68/FenixSyncShared"
	"github.com/sirupsen/logrus"
)

// Used for only process cleanup once
var cleanupProcessed bool = false

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

func FenixClientServer_main() {

	// Connect to CloudDB
	fenixSyncShared.ConnectToDB()

	// Set up BackendObject
	fenixClientTestDataSyncServerObject = &fenixClientTestDataSyncServerObject_struct{}

	// Init logger
	fenixClientTestDataSyncServerObject.InitLogger("")

	// TODO Endast för Test
	fenixClientTestDataSyncServerObject.loadAllTestDataRowItemsForClientFromCloudDB(&cloudDBTestDataRowItems)
	fenixClientTestDataSyncServerObject.loadClientInfoFromCloudDB(&cloudDBClientInfo)

	// Clean up when leaving. Is placed after logger because shutdown logs information
	defer cleanup()

	// Start Backend gRPC-server
	fenixClientTestDataSyncServerObject.InitGrpcServer()

}
