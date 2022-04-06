package main

import (
	"FenixClientServer/common_config"
	"crypto/tls"
	"fmt"
	fenixClientTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Client/fenixClientTestDataSyncServerGrpcApi/go_grpc_api"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcMetadata "google.golang.org/grpc/metadata"
	"log"
	"os"
	"time"
)

// ********************************************************************************************************************

// SetConnectionToFenixTestDataSyncServer - Set upp connection and Dial to FenixTestDataSyncServer
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SetConnectionToFenixTestDataSyncServer() {

	var err error
	var opts []grpc.DialOption

	//When running on GCP then use credential otherwise not
	if common_config.ExecutionLocationForFenixTestDataServer == common_config.GCP {
		creds := credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})

		opts = []grpc.DialOption{
			grpc.WithTransportCredentials(creds),
		}
	}

	// Set up connection to FenixTestDataSyncServer
	// When run on GCP, use credentials
	if common_config.ExecutionLocationForFenixTestDataServer == common_config.GCP {
		// Run on GCP
		remoteFenixTestDataSyncServerConnection, err = grpc.Dial(fenixTestDataSyncServer_address_to_dial, opts...)
	} else {
		// Run Local
		remoteFenixTestDataSyncServerConnection, err = grpc.Dial(fenixTestDataSyncServer_address_to_dial, grpc.WithInsecure())
	}
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "50b59b1b-57ce-4c27-aa84-617f0cde3100",
			"fenixTestDataSyncServer_address_to_dial": fenixTestDataSyncServer_address_to_dial,
			"error message": err,
		}).Error("Did not connect to FenixTestDataSyncServer via gRPC")
		//os.Exit(0)
	} else {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "0c650bbc-45d0-4029-bd25-4ced9925a059",
			"fenixTestDataSyncServer_address_to_dial": fenixTestDataSyncServer_address_to_dial,
		}).Info("gRPC connection OK to FenixTestDataSyncServer")

		// Creates a new Clients
		fenixTestDataSyncServerClient = fenixTestDataSyncServerGrpcApi.NewFenixTestDataGrpcServicesClient(remoteFenixTestDataSyncServerConnection)

	}
}

// ********************************************************************************************************************

// Get the highest FenixProtoFileVersionEnumeration
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) getHighestFenixProtoFileVersion() int32 {

	// Check if there already is a 'highestFenixProtoFileVersion' saved, if so use that one
	if highestFenixProtoFileVersion != -1 {
		return highestFenixProtoFileVersion
	}

	// Find the highest value for proto-file version
	var maxValue int32
	maxValue = 0

	for _, v := range fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum_value {
		if v > maxValue {
			maxValue = v
		}
	}

	highestFenixProtoFileVersion = maxValue

	return highestFenixProtoFileVersion
}

// ********************************************************************************************************************
// Get the highest ClientProtoFileVersionEnumeration
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) getHighestClientProtoFileVersion() int32 {

	// Check if there already is a 'highestclientProtoFileVersion' saved, if so use that one
	if highestClientProtoFileVersion != -1 {
		return highestClientProtoFileVersion
	}

	// Find the highest value for proto-file version
	var maxValue int32
	maxValue = 0

	for _, v := range fenixClientTestDataSyncServerGrpcApi.CurrentFenixClientTestDataProtoFileVersionEnum_value {
		if v > maxValue {
			maxValue = v
		}
	}

	highestClientProtoFileVersion = maxValue

	return highestClientProtoFileVersion
}

// ********************************************************************************************************************
// Check if Calling 'system' (Fenix or Clients own methods) is using correct proto-file version
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) isCallerUsingCorrectProtoFileVersion(
	usedProtoFileVersion fenixClientTestDataSyncServerGrpcApi.CurrentFenixClientTestDataProtoFileVersionEnum) (
	clientUseCorrectProtoFileVersion bool,
	protoFileExpected fenixClientTestDataSyncServerGrpcApi.CurrentFenixClientTestDataProtoFileVersionEnum,
	protoFileUsed fenixClientTestDataSyncServerGrpcApi.CurrentFenixClientTestDataProtoFileVersionEnum) {

	protoFileUsed = usedProtoFileVersion
	protoFileExpected = fenixClientTestDataSyncServerGrpcApi.CurrentFenixClientTestDataProtoFileVersionEnum(
		fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion())

	// Check if correct proto files is used
	if protoFileExpected == protoFileUsed {
		clientUseCorrectProtoFileVersion = true
	} else {
		clientUseCorrectProtoFileVersion = true
	}

	//protoFileExpectedDescription := protoFileExpected.String()
	//protoFileExpectedDescription := protoFileExpected.String()

	return clientUseCorrectProtoFileVersion, protoFileExpected, protoFileUsed
}

// Generate the current MerkleTree for Testdata supported by the client
func getCurrentTestDataMerkleTree() fenixTestDataSyncServerGrpcApi.MerkleTreeMessage {

	var merkleTreeMessage fenixTestDataSyncServerGrpcApi.MerkleTreeMessage

	return merkleTreeMessage
}

// ********************************************************************************************************************

// RegisterTestDataClient  - Register the client at Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) createTestDataHeaderMessage() *fenixTestDataSyncServerGrpcApi.TestDataHeadersMessage {

	var testDataHeaderItemMessage *fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage
	var testDataHeaderItemsMessage []*fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage
	_, _, testDataHeaders := common_config.LoadAndProcessFile(testFile)

	var testDataHeaderItemMessageHashArray []string

	// Extract Header names, from sub set of testdata(1 row)
	testDataSubSet := testDataHeaders.Subset(0)
	headerData := testDataSubSet.Records()[0]

	// Create variables to be sent to FenixTestDataSyncServer
	for _, header := range headerData {
		if header != "TestDataHash" {
			var headerFilterValues []*fenixTestDataSyncServerGrpcApi.HeaderFilterValue
			headerFilterValue := &fenixTestDataSyncServerGrpcApi.HeaderFilterValue{HeaderFilterValuesAsString: "value 1"}
			headerFilterValues = append(headerFilterValues, headerFilterValue)
			testDataHeaderItemMessage = &fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage{
				TestDataHeaderItemMessageHash:       "XXX Is set below in the code XXX",
				HeaderLabel:                         header,
				HeaderShouldBeUsedForTestDataFilter: false,
				HeaderIsMandatoryInTestDataFilter:   false,
				HeaderSelectionType:                 fenixTestDataSyncServerGrpcApi.HeaderSelectionTypeEnum_HEADER_IS_SINGLE_SELECT,
				HeaderFilterValues:                  headerFilterValues,
			}
			// Add hash value to 'TestDataHeaderItemMessageHash'
			testDataHeaderItemMessageHash := common_config.CreateTestDataHeaderItemMessageHash(testDataHeaderItemMessage)
			testDataHeaderItemMessage.TestDataHeaderItemMessageHash = testDataHeaderItemMessageHash

			testDataHeaderItemsMessage = append(testDataHeaderItemsMessage, testDataHeaderItemMessage)
			testDataHeaderItemMessageHashArray = append(testDataHeaderItemMessageHashArray, testDataHeaderItemMessageHash)

		}

	}

	// Hash all 'testDataHeaderItemMessageHash' into a single hash
	testDataHeaderItemMessageHash := common_config.HashValues(testDataHeaderItemMessageHashArray, false)

	// Header message to be set to  TestDataSyncServer
	testDataHeaderMessage := &fenixTestDataSyncServerGrpcApi.TestDataHeadersMessage{
		TestDataClientUuid:      fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		TestDataHeaderItemsHash: testDataHeaderItemMessageHash,
		TestDataHeaderItems:     testDataHeaderItemsMessage,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(
			fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	return testDataHeaderMessage

}

// ********************************************************************************************************************

// RegisterTestDataClient  - Register the client at Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) RegisterTestDataClient() {

	// Set up variables to be sent to FenixTestDataSyncServer
	TestDataClientInformationMessage := fenixTestDataSyncServerGrpcApi.TestDataClientInformationMessage{
		TestDataClientUuid:           fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		TestDomainUuid:               fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_DomainUuid,
		TestDomainName:               fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_DomainName,
		TestDataClientIpAddress:      common_config.ClientTestDataSyncServerAddress,
		TestDataClientPort:           string(rune(common_config.ClientTestDataSyncServerPort)),
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.RegisterTestDataClient(ctx, &TestDataClientInformationMessage)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "6b080a23-4e06-4d16-8295-a67ba7115a56",
			"error": err,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'RegisterTestDataClient'")

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "44671efb-e24d-450a-acba-006cc248d058",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'RegisterTestDataClient'")
	}

}

// ********************************************************************************************************************

// SendMerkleHash - Send the client's MerkleHash to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendMerkleHash() {

	merkleRootHash, _, _ := common_config.LoadAndProcessFile(testFile)
	merkleFilterPathHash := common_config.HashSingleValue(fenixClientTestDataSyncServerObject.merkleFilterPath)

	// Set up variables to be sent to FenixTestDataSyncServer
	merkleHashMessage := fenixTestDataSyncServerGrpcApi.MerkleHashMessage{
		TestDataClientUuid: fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		MerkleHash:         merkleRootHash,
		MerkleFilter:       fenixClientTestDataSyncServerObject.merkleFilterPath,
		MerkleFilterHash:   merkleFilterPathHash,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(
			fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.SendMerkleHash(ctx, &merkleHashMessage)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "69a62788-b798-471a-bb8d-7fa1cec0f485",
			"error": err,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleHash'")

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "fb923a55-136e-481e-9c30-d7d7019e17e3",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleHash'")
	} else {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":         "a9b9578e-4bc7-4ef5-91b6-67a8191a0af5",
			"MerkleHash": merkleRootHash,
		}).Debug("Sent MerkleHash to Server")
	}

}

// ********************************************************************************************************************

// SendMerkleTree - Send the client's MerkleTree to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendMerkleTree() {

	var merkleTreeNodeMessages []*fenixTestDataSyncServerGrpcApi.MerkleTreeNodeMessage

	// Set up variables to be sent to FenixTestDataSyncServer
	_, merkleTree, _ := common_config.LoadAndProcessFile(testFile)

	f, err := os.Create("merkleTreeProduced.csv")
	if err != nil {
		log.Fatal(err)
	}

	merkleTree.WriteCSV(f)
	f.Close()

	merkleTreeNRows := merkleTree.Nrow()
	for rowCounter := 0; rowCounter < merkleTreeNRows; rowCounter++ {
		merkleLevel, _ := merkleTree.Elem(rowCounter, 0).Int()
		merkleTreeNodeMessage := &fenixTestDataSyncServerGrpcApi.MerkleTreeNodeMessage{
			NodeLevel:     uint32(merkleLevel),                     //MerkleLevel (0)
			NodeName:      merkleTree.Elem(rowCounter, 1).String(), // MerkleName (1)
			NodePath:      merkleTree.Elem(rowCounter, 4).String(), // MerkleFilterPath (4)
			NodeHash:      merkleTree.Elem(rowCounter, 2).String(), // MerkleHash (2)
			NodeChildHash: merkleTree.Elem(rowCounter, 3).String(), // MerkleChildHash (3)
		}

		merkleTreeNodeMessages = append(merkleTreeNodeMessages, merkleTreeNodeMessage)
	}
	merkleTreeMessage := &fenixTestDataSyncServerGrpcApi.MerkleTreeMessage{
		TestDataClientUuid: fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		MerkleTreeNodes:    merkleTreeNodeMessages,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(
			fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.SendMerkleTree(ctx, merkleTreeMessage)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "c8a66468-17ca-4e0a-942b-a9ec9b246c82",
			"error": err,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleTree'")

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "d8225481-d28c-426c-9cdb-986678001e5c",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleTree'")
	}

}

// ********************************************************************************************************************

// SendTestDataHeaderHash - Send the client's TestDataHeaderHash to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendTestDataHeaderHash() {

	var testDataHeaderItemMessage *fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage
	var testDataHeaderItemsMessage []*fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage
	_, _, testDataHeaders := common_config.LoadAndProcessFile(testFile)
	var testDataHeaderItemMessageHashArray []string

	// Extract Header names, from sub set of testdata(1 row)
	testDataSubSet := testDataHeaders.Subset(0)
	headerData := testDataSubSet.Records()[0]

	// Create variables to be sent to FenixTestDataSyncServer
	for _, header := range headerData {
		if header != "TestDataHash" {
			var headerFilterValues []*fenixTestDataSyncServerGrpcApi.HeaderFilterValue
			headerFilterValue := &fenixTestDataSyncServerGrpcApi.HeaderFilterValue{HeaderFilterValuesAsString: "value 1"}
			headerFilterValues = append(headerFilterValues, headerFilterValue)
			testDataHeaderItemMessage = &fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage{
				TestDataHeaderItemMessageHash:       "XXX Is set below in the code XXX",
				HeaderLabel:                         header,
				HeaderShouldBeUsedForTestDataFilter: false,
				HeaderIsMandatoryInTestDataFilter:   false,
				HeaderSelectionType:                 fenixTestDataSyncServerGrpcApi.HeaderSelectionTypeEnum_HEADER_IS_SINGLE_SELECT,
				HeaderFilterValues:                  headerFilterValues,
			}
			// Add hash value to 'TestDataHeaderItemMessageHash'
			testDataHeaderItemMessageHash := common_config.CreateTestDataHeaderItemMessageHash(testDataHeaderItemMessage)
			testDataHeaderItemMessage.TestDataHeaderItemMessageHash = testDataHeaderItemMessageHash

			testDataHeaderItemsMessage = append(testDataHeaderItemsMessage, testDataHeaderItemMessage)
			testDataHeaderItemMessageHashArray = append(testDataHeaderItemMessageHashArray, testDataHeaderItemMessageHash)

		}

	}

	// Hash all 'testDataHeaderItemMessageHash' into a single hash
	testDataHeaderItemMessageHash := common_config.HashValues(testDataHeaderItemMessageHashArray, false)

	// HeaderHash message to be set to TestDataSyncServer
	testDataHeaderMessage := &fenixTestDataSyncServerGrpcApi.TestDataHeaderHashMessage{
		TestDataClientUuid:      fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		TestDataHeaderItemsHash: testDataHeaderItemMessageHash,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(
			fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.SendTestDataHeaderHash(ctx, testDataHeaderMessage)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "0f8d37a3-ac0d-4096-a5a7-c635cd434926",
			"error": err,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataHeaders'")

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "1a2a215f-6356-49a5-a7db-e9a9ead2fe6e",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataHeaders'")
	}

}

// ********************************************************************************************************************

// SendTestDataHeaders - Send the client's TestDataHeaders to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendTestDataHeaders() {

	// Header message to be sent to  TestDataSyncServer
	testDataHeaderMessage := fenixClientTestDataSyncServerObject.createTestDataHeaderMessage()

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.SendTestDataHeaders(ctx, testDataHeaderMessage)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "5644eeb0-7e95-4b42-ae2a-1fafdf926f9d",
			"error": err,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataHeaders'")

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "3902e0d2-d28a-40e4-8aa8-553d31ac3b78",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataHeaders'")
	}

}

// TODO - fix so fkn can take which rows to send back
// ********************************************************************************************************************

// SendTestDataRows - Send the client's TestDataRow to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendTestDataRows(merkleNodeNames []string) {

	// Create the message with all test data to be sent to Fenix
	testdataRowsMessages := fenixClientTestDataSyncServerObject.createRowsMessage(merkleNodeNames)

	// Create the message that will all messages to be in the stream
	var testdataRowsMessagesStreamContainer []*fenixTestDataSyncServerGrpcApi.TestdataRowsMessages
	testdataRowsMessagesStreamContainer = append(testdataRowsMessagesStreamContainer, testdataRowsMessages)

	/*
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":                                  "5644eeb0-7e95-4b42-ae2a-1fafdf926f9d",
			"testdataRowsMessagesStreamContainer": testdataRowsMessagesStreamContainer,
		}).Debug("Sent TestDataRows")
	*/

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Set up Stream towards the gRPC-server
	ctx := context.Background()
	//ctx2, _ := context.WithCancel(ctx)
	//defer cancel()
	stream, err := fenixTestDataSyncServerClient.SendTestDataRows(ctx)
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "e5d20ade-539b-4c20-834a-628e131d06be",
			"error": err,
		}).Error("Couldn't set up a 'Stream' to gRPC-server ")

		return
	}

	// Loop all 'testdataRowsMessages' and stream the messages to the gRPC-server
	for counter, testdataRowsMessages := range testdataRowsMessagesStreamContainer {
		err = stream.Send(testdataRowsMessages)
		if err != nil {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":    "4773d81c-f42d-46d5-8d54-86a90c3d717f",
				"error": err,
			}).Error("Couldn't send stream-object to gRPC-server ")

			//return
		}
		fmt.Println(counter)

	}
	returnMessage, err := stream.CloseAndRecv()

	// Do gRPC-call
	//ctx := context.Background()
	//returnMessage, err := fenixTestDataSyncServerClient.SendTestDataRows(ctx, testdataRowsMessages)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":            "b457b233-41f9-4b3d-9f1e-00782b467045",
			"error":         err,
			"returnMessage": returnMessage,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataRows'")

		return

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "c1f6a351-fb7e-4759-81a7-04ec61b74e59",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataRows'")

		return
	} else {

		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "ef4afef1-6307-4f42-9623-4434040cc7da",
			"Message from FenixTestDataSyncServer: 'returnMessage'": returnMessage,
			"Messages Sent": len(testdataRowsMessagesStreamContainer),
		}).Debug("Send TestDataRowMessages'")
	}

}

// ********************************************************************************************************************

// SendAreYouAliveToFenixTestDataServer - Send the client's TestDataHeaders to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendAreYouAliveToFenixTestDataServer() (bool, string) {

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Create the message with all test data to be sent to Fenix
	emptyParameter := &fenixTestDataSyncServerGrpcApi.EmptyParameter{

		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Do gRPC-call
	//ctx := context.Background()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer func() {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "baee9a42-2f70-4977-8c35-f5f7850bbd3a",
		}).Error("Running Defer Cancel function")
		cancel()
	}()

	// Only add access token when run on GCP
	if common_config.ExecutionLocationForFenixTestDataServer == common_config.GCP {

		// Create an identity token.
		// With a global TokenSource tokens would be reused and auto-refreshed at need.
		// A given TokenSource is specific to the audience.
		tokenSource, err := idtoken.NewTokenSource(ctx, common_config.ClientTestDataSyncServerAddress+":443")
		if err != nil {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":  "8ba622d8-b4cd-46c7-9f81-d9ade2568eca",
				"err": err,
			}).Error("Couldn't generate access token")

			return false, "Couldn't generate access token"
		}

		token, err := tokenSource.Token()
		if err != nil {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":  "0cf31da5-9e6b-41bc-96f1-6b78fb446194",
				"err": err,
			}).Error("Problem getting the token")

			return false, "Problem getting the token"
		} else {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":    "8b1ca089-0797-4ee6-bf9d-f9b06f606ae9",
				"token": token,
			}).Debug("Got Bearer Token")
		}

		// Add token to gRPC Request.
		ctx = grpcMetadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token.AccessToken)
	}

	returnMessage, err := fenixTestDataSyncServerClient.AreYouAlive(ctx, emptyParameter)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "818aaf0b-4112-4be4-97b9-21cc084c7b8b",
			"error": err,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendAreYouAliveToFenixTestDataServer'")

	} else if returnMessage.AckNack == false {
		// FenixTestDataSyncServer couldn't handle gPRC call
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID": "2ecbc800-2fb6-4e88-858d-a421b61c5529",
			"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
		}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendAreYouAliveToFenixTestDataServer'")
	}

	return returnMessage.AckNack, returnMessage.Comments

}
