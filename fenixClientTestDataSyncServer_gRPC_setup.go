package main

import (
	fenixClientTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Client/fenixClientTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"strconv"
)

// Set up and start Backend gRPC-server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) InitGrpcServer() {

	var err error

	// Find first non allocated port from defined start port
	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"Id": "054bc0ef-93bb-4b75-8630-74e3823f71da",
	}).Info("Backend Server tries to start")
	for counter := 0; counter < 10; counter++ {
		localServerEngineLocalPort = localServerEngineLocalPort + counter
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"Id":                           "ca3593b1-466b-4536-be91-5e038de178f4",
			"localServerEngineLocalPort: ": localServerEngineLocalPort,
		}).Info("Start listening on:")
		lis, err = net.Listen("tcp", ":"+strconv.Itoa(localServerEngineLocalPort))

		if err != nil {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"Id":    "ad7815b3-63e8-4ab1-9d4a-987d9bd94c76",
				"err: ": err,
			}).Error("failed to listen:")
		} else {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"Id":                           "ba070b9b-5d57-4c0a-ab4c-a76247a50fd3",
				"localServerEngineLocalPort: ": localServerEngineLocalPort,
			}).Info("Success in listening on port:")

			break
		}
	}

	// Creates a new RegisterWorkerServer gRPC server
	//go func() {
	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"Id": "b0ccffb5-4367-464c-a3bc-460cafed16cb",
	}).Info("Starting Backend gRPC Server")

	registerfenixClientTestDataSyncServerServer = grpc.NewServer()
	fenixClientTestDataSyncServerGrpcApi.RegisterFenixClientTestDataGrpcServicesServer(registerfenixClientTestDataSyncServerServer, &FenixClientTestDataGrpcServicesServer{})

	// Register RouteGuide on the same server.
	//reflection.Register(registerfenixClientTestDataSyncServerServer)

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"Id":                           "e843ece9-b707-4c60-b1d8-14464305e68f",
		"localServerEngineLocalPort: ": localServerEngineLocalPort,
	}).Info("registerfenixClientTestDataSyncServerServer for TestInstruction Backend Server started")
	registerfenixClientTestDataSyncServerServer.Serve(lis)
	//}()

}

// Stop Backend gRPC-server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) StopGrpcServer() {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{}).Info("Gracefull stop for: registerTaxiHardwareStreamServer")
	registerfenixClientTestDataSyncServerServer.GracefulStop()

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"localServerEngineLocalPort: ": localServerEngineLocalPort,
	}).Info("Close net.Listing")
	_ = lis.Close()

}
