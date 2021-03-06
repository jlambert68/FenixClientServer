package main

import (
	fenixClientTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Client/fenixClientTestDataSyncServerGrpcApi/go_grpc_api"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"net"
	//	ecpb "github.com/jlambert68/FenixGrpcApi/Client/fenixTestDataSyncServerGrpcApi/echo/go_grpc_api"
)

type fenixClientTestDataSyncServerObject_struct struct {
	logger                                           *logrus.Logger
	fenixClientTestDataSyncServer_TestDataClientUuid string
	fenixClientTestDataSyncServer_DomainUuid         string
	fenixClientTestDataSyncServer_DomainName         string
	merkleFilterPath                                 string
	gcpAccessToken                                   *oauth2.Token
}

var fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct

// Global connection constants
//var localServerEngineLocalPort int

var (
	registerfenixClientTestDataSyncServerServer *grpc.Server
	lis                                         net.Listener
)

var (
	// Standard gRPC Clientr
	remoteFenixTestDataSyncServerConnection *grpc.ClientConn
	gRpcClientForFenixTestDataSyncServer    fenixTestDataSyncServerGrpcApi.FenixTestDataGrpcServicesClient

	fenixTestDataSyncServer_address_to_dial string

	fenixTestDataSyncServerClient fenixTestDataSyncServerGrpcApi.FenixTestDataGrpcServicesClient
)

// Server used for register clients Name, Ip and Por and Clients Test Enviroments and Clients Test Commandst
type FenixClientTestDataGrpcServicesServer struct {
	fenixClientTestDataSyncServerGrpcApi.UnimplementedFenixClientTestDataGrpcServicesServer
}

//TODO FIXA DENNA PATH, HMMM borde köra i DB framöver
// For now hardcoded MerklePath
//var merkleFilterPath string = //"AccountEnvironment/ClientJuristictionCountryCode/MarketSubType/MarketName/" //SecurityType/"

var testFile_1 = "data/FenixRawTestdata_14rows_211216.csv"

var testFile_2 = "data/FenixRawTestdata_14rows_211216_change.csv"

var testFileSelection bool = true

var testFile = testFile_2

var highestFenixProtoFileVersion int32 = -1
var highestClientProtoFileVersion int32 = -1

// Echo gRPC-server
/*
type ecServer struct {
	echo.UnimplementedEchoServer
}


*/
