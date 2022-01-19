package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
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

	// Set up BackendObject
	fenixClientTestDataSyncServerObject = &fenixClientTestDataSyncServerObject_struct{
		iAmBusy:               false,
		qmlServerHasConnected: false}

	// Create unique id for this Backend Server
	uuId, _ := uuid.NewUUID()
	fmt.Println(uuId)
	fenixClientTestDataSyncServerObject.uuid = uuId.String()

	// Init logger
	fenixClientTestDataSyncServerObject.InitLogger("")

	// Celan up when leaving. Is placed after logger because shutdown logs information
	//defer cleanup()

	// Start Backend gRPC-server
	fenixClientTestDataSyncServerObject.InitGrpcServer()

	// Register at QML Server
	// TODO Detta ska inte göras. Denna komponent ska vara passiv
	//fenixClientTestDataSyncServerObject.SendMQmlServerIpAndPortForBackendServer()

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		cleanup()
		os.Exit(0)
	}()

	//for {
	fmt.Println("sleeping...for another 3 second 5 minutes")
	time.Sleep(3 * time.Second) // or runtime.Gosched() or similar per @misterbee
	//}

	//Wait until user exit
	/*
		   for {
			   time.Sleep(10)
		   }
	*/
}
