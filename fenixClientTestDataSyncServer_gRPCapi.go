package main

import (
	fenixClientTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Client/fenixClientTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// *********************************************************************
//Fenix client can check if Fenix Testdata sync server is alive with this service
func (s *FenixClientTestDataGrpcServicesServer) AreYouAlive(ctx context.Context, emptyParameter *fenixClientTestDataSyncServerGrpcApi.EmptyParameter) (*fenixClientTestDataSyncServerGrpcApi.AckNackResponse, error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "1ff67695-9a8b-4821-811d-0ab8d33c4d8b",
	}).Debug("Incoming 'AreYouAlive'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "9c7f0c3d-7e9f-4c91-934e-8d7a22926d84",
	}).Debug("Outgoing 'AreYouAlive'")

	return &fenixClientTestDataSyncServerGrpcApi.AckNackResponse{Acknack: true, Comments: "I'am alive"}, nil
}

// *********************************************************************
// Fenix client can register itself with the Fenix Testdata sync server
func (s *FenixClientTestDataGrpcServicesServer) SendMerkleHash(ctx context.Context, merkleHashMessage *fenixClientTestDataSyncServerGrpcApi.EmptyParameter) (*fenixClientTestDataSyncServerGrpcApi.AckNackResponse, error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "a55f9c82-1d74-44a5-8662-058b8bc9e48f",
	}).Debug("Incoming 'SendMerkleHash'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "27fb45fe-3266-41aa-a6af-958513977e28",
	}).Debug("Outgoing 'SendMerkleHash'")

	// Send MerkleHash to Fenix after sending return message back to caller
	fenixClientTestDataSyncServerObject.SendMerkleHash()

	return &fenixClientTestDataSyncServerGrpcApi.AckNackResponse{Acknack: true, Comments: ""}, nil
}

// *********************************************************************
// Fenix client can send TestData MerkleTree to Fenix Testdata sync server with this service
func (s *FenixClientTestDataGrpcServicesServer) SendMerkleTree(ctx context.Context, merkleTreeMessage *fenixClientTestDataSyncServerGrpcApi.EmptyParameter) (*fenixClientTestDataSyncServerGrpcApi.AckNackResponse, error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "cffc25f0-b0e6-407a-942a-71fc74f831ac",
	}).Debug("Incoming 'SendMerkleTree'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "61e2c28d-b091-442a-b7f8-d2502d9547cf",
	}).Debug("Outgoing 'SendMerkleTree'")

	// Send MerkleTree to Fenix after sending return message back to caller
	defer fenixClientTestDataSyncServerObject.SendMerkleTree()

	return &fenixClientTestDataSyncServerGrpcApi.AckNackResponse{Acknack: true, Comments: ""}, nil
}

// *********************************************************************
// Fenix client can send TestDataHeaders to Fenix Testdata sync server with this service
func (s *FenixClientTestDataGrpcServicesServer) SendTestDataHeaders(ctx context.Context, testDataHeaderMessage *fenixClientTestDataSyncServerGrpcApi.EmptyParameter) (*fenixClientTestDataSyncServerGrpcApi.AckNackResponse, error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "aee48999-12ad-4bb7-bc8a-96b62a8eeedf",
	}).Debug("Incoming 'SendTestDataHeaders'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "ca0b58a8-6d56-4392-8751-45906670e86b",
	}).Debug("Outgoing 'SendTestDataHeaders'")

	// Send TestDataHeaders to Fenix after sending return message back to caller
	defer fenixClientTestDataSyncServerObject.SendTestDataHeaders()

	return &fenixClientTestDataSyncServerGrpcApi.AckNackResponse{Acknack: true, Comments: ""}, nil
}

// *********************************************************************
// Fenix client can send TestData rows to Fenix Testdata sync server with this service
func (s *FenixClientTestDataGrpcServicesServer) SendTestDataRows(ctx context.Context, merkleTreeMessage *fenixClientTestDataSyncServerGrpcApi.MerklePathsMessage) (*fenixClientTestDataSyncServerGrpcApi.AckNackResponse, error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "2b1c8752-eb84-4c15-b8a7-22e2464e5168",
	}).Debug("Incoming 'SendTestDataRows'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "755e8b4f-f184-4277-ad41-e041714c2ca8",
	}).Debug("Outgoing 'SendTestDataRows'")

	// Send TestDataRows to Fenix after sending return message back to caller
	defer fenixClientTestDataSyncServerObject.SendTestDataRows()

	return &fenixClientTestDataSyncServerGrpcApi.AckNackResponse{Acknack: true, Comments: ""}, nil
}

// Fenix client can register itself with the Fenix Testdata sync server
func (s *FenixClientTestDataGrpcServicesServer) RegisterTestDataClient(ctx context.Context, testDataClientInformationMessage *fenixClientTestDataSyncServerGrpcApi.EmptyParameter) (*fenixClientTestDataSyncServerGrpcApi.AckNackResponse, error) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "5133b80b-6f3a-4562-9e62-1b3ceb169cc1",
	}).Debug("Incoming 'RegisterTestDataClient'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "316dcd7e-2229-4a82-b15b-0f808c2dd8aa",
	}).Debug("Outgoing 'RegisterTestDataClient'")

	// Send Client registration to Fenix after sending return message back to caller
	defer fenixClientTestDataSyncServerObject.SendMerkleHash()

	return &fenixClientTestDataSyncServerGrpcApi.AckNackResponse{Acknack: true, Comments: ""}, nil
}

/*
func (s *FenixClientTestDataGrpcServicesServer) mustEmbedUnimplementedFenixClientTestDataGrpcServicesServer() {
	//TODO implement me
	panic("implement me")
}


*/
