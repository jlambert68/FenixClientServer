package main

import (
	"FenixClientServer/common_config"
	"crypto/tls"
	"github.com/go-gota/gota/dataframe"
	fenixClientTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Client/fenixClientTestDataSyncServerGrpcApi/go_grpc_api"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"os"
)

// ********************************************************************************************************************
// Set upp connection and Dial to FenixTestDataSyncServer
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SetConnectionToFenixTestDataSyncServer() {

	var err error

	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
	})

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}

	// Set up connection to FenixTestDataSyncServer
	remoteFenixTestDataSyncServerConnection, err = grpc.Dial(fenixTestDataSyncServer_address_to_dial, opts...)
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"fenixTestDataSyncServer_address_to_dial": fenixTestDataSyncServer_address_to_dial,
			"error message": err,
		}).Error("Did not connect to FenixTestDataSyncServer via gRPC")
		//os.Exit(0)
	} else {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
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
// Register the client at Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) RegisterTestDataClient() {

	// Set up variables to be sent to FenixTestDataSyncServer
	TestDataClientInformationMessage := fenixTestDataSyncServerGrpcApi.TestDataClientInformationMessage{
		TestDataClientGuid:           common_config.FenicClientTestDataSyncServer_TestDataClientGuid,
		TestDomainGuid:               common_config.FenicClientTestDataSyncServer_DomainGuid,
		TestDomainName:               common_config.FenicClientTestDataSyncServer_DomainName,
		TestDataClientIpAddress:      common_config.FenixClientTestDataSyncServer_address,
		TestDataClientPort:           string(common_config.FenixClientTestDataSyncServer_initial_port),
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
		}).Fatal("Problem to do gRPC-call to FenixTestDataSyncServer for 'RegisterTestDataClient'")

		// FenixTestDataSyncServer couldn't handle gPRC call
		if returnMessage.Acknack == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID": "44671efb-e24d-450a-acba-006cc248d058",
				"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
			}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'RegisterTestDataClient'")
		}
	}

}

// ********************************************************************************************************************
// Send the client's MerkleHash to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendMerkleHash() {

	merkleRootHash, _ := common_config.LoadAndProcessFile(testFile)

	// Set up variables to be sent to FenixTestDataSyncServer
	merkleHashMessage := fenixTestDataSyncServerGrpcApi.MerkleHashMessage{
		TestDataClientGuid: common_config.FenicClientTestDataSyncServer_TestDataClientGuid,
		MerkleHash:         merkleRootHash,
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
		}).Fatal("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleHash'")

		// FenixTestDataSyncServer couldn't handle gPRC call
		if returnMessage.Acknack == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID": "fb923a55-136e-481e-9c30-d7d7019e17e3",
				"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
			}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleHash'")
		}
	}

}

// ********************************************************************************************************************
// Send the client's MerkleTree to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendMerkleTree() {

	var merkleTreeNodeMessages []*fenixTestDataSyncServerGrpcApi.MerkleTreeNodeMessage

	// Set up variables to be sent to FenixTestDataSyncServer
	_, merkleTree := common_config.LoadAndProcessFile(testFile)

	merkleTreeNRows := merkleTree.Nrow()
	for rowCounter := 0; rowCounter < merkleTreeNRows; rowCounter++ {
		merkleLevel, _ := merkleTree.Elem(rowCounter, 0).Int()
		merkleTreeNodeMessage := &fenixTestDataSyncServerGrpcApi.MerkleTreeNodeMessage{
			MerkleLevel:     int64(merkleLevel),
			MerklePath:      merkleTree.Elem(rowCounter, 1).String(),
			MerkleHash:      merkleTree.Elem(rowCounter, 2).String(),
			MerkleChildHash: merkleTree.Elem(rowCounter, 3).String(),
		}
		merkleTreeNodeMessages = append(merkleTreeNodeMessages, merkleTreeNodeMessage)
	}
	merkleTreeMessage := &fenixTestDataSyncServerGrpcApi.MerkleTreeMessage{
		TestDataClientGuid: common_config.FenicClientTestDataSyncServer_TestDataClientGuid,
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
		}).Fatal("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleTree'")

		// FenixTestDataSyncServer couldn't handle gPRC call
		if returnMessage.Acknack == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID": "d8225481-d28c-426c-9cdb-986678001e5c",
				"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
			}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendMerkleTree'")
		}
	}

}

// ********************************************************************************************************************
// Send the client's TestDataHeaders to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendTestDataHeaders() {

	var testDataHeaderItemMessage *fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage
	var testDataHeaderItemsMessage []*fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage
	_, merkleTree := common_config.LoadAndProcessFile(testFile)

	// Get all headers as a string array
	headers := merkleTree.Names()

	// Hash the header into a single hash
	// TODO Redo how HeaderHash is calculated, all filter values must be included
	headerHash := common_config.HashValues(headers)

	// Create variables to be sent to FenixTestDataSyncServer
	for _, header := range headers {
		var headerFilterValues []*fenixTestDataSyncServerGrpcApi.HeaderFilterValue
		headerFilterValue := &fenixTestDataSyncServerGrpcApi.HeaderFilterValue{HeaderFilterValuesAsString: "value 1"}
		headerFilterValues = append(headerFilterValues, headerFilterValue)
		testDataHeaderItemMessage = &fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage{
			HeaderPresentationsLabel:             header,
			HeaderDataLabel:                      header,
			HeaderShouldbBeUsedForTestDataFilter: false,
			HeaderIsMandatoryInTestDataFilter:    false,
			HeaderSelectionType:                  fenixTestDataSyncServerGrpcApi.HeaderSelectionTypeEnum_HEADER_IS_SINGLE_SELECT,
			HeaderFilterValues:                   headerFilterValues,
		}

		testDataHeaderItemsMessage = append(testDataHeaderItemsMessage, testDataHeaderItemMessage)
	}

	// Header message to be set to  TestDataSyncServer
	testDataHeaderMessage := &fenixTestDataSyncServerGrpcApi.TestDataHeaderMessage{
		TestDataClientGuid:  common_config.FenicClientTestDataSyncServer_TestDataClientGuid,
		HeadersHash:         headerHash,
		TestDataHeaderItems: testDataHeaderItemsMessage,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(
			fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

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
		}).Fatal("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataHeaders'")

		// FenixTestDataSyncServer couldn't handle gPRC call
		if returnMessage.Acknack == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID": "3902e0d2-d28a-40e4-8aa8-553d31ac3b78",
				"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
			}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataHeaders'")
		}
	}

}

// TODO - fix so fkn can take which rows to send back
// ********************************************************************************************************************
// Send the client's TestDataRow to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendTestDataRows() {

	var testdataRowsMessages *fenixTestDataSyncServerGrpcApi.TestdataRowsMessages
	var testdataRows []*fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var testDataRowMessage *fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var testdataItemMessage *fenixTestDataSyncServerGrpcApi.TestDataItemMessage

	var testDataItemValueAsString string

	// Load Testdata file
	irisCsv, err := os.Open("data/FenixRawTestdata_14rows_211216.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer irisCsv.Close()

	df := dataframe.ReadCSV(irisCsv,
		dataframe.WithDelimiter(';'),
		dataframe.HasHeader(true))

	number_of_columns_to_process := df.Ncol()
	numberOfRows := df.Nrow()
	for rowCounter := 0; rowCounter < numberOfRows; rowCounter++ {

		var valuesToHash []string
		var testdataItems []*fenixTestDataSyncServerGrpcApi.TestDataItemMessage

		for columnCounter := 0; columnCounter < number_of_columns_to_process; columnCounter++ {
			// add values for one row
			testDataItemValueAsString = df.Elem(rowCounter, columnCounter).String()
			testdataItemMessage = &fenixTestDataSyncServerGrpcApi.TestDataItemMessage{
				TestDataItemValueAsString: testDataItemValueAsString,
			}
			testdataItems = append(testdataItems, testdataItemMessage)
			valuesToHash = append(valuesToHash, testDataItemValueAsString)
		}

		// Hash all values for row
		hashedRow := common_config.HashValues(valuesToHash)

		// Create one row object and add it to array
		testDataRowMessage = &fenixTestDataSyncServerGrpcApi.TestDataRowMessage{
			RowHash:       hashedRow,
			TestDataItems: testdataItems,
		}
		testdataRows = append(testdataRows, testDataRowMessage)

	}

	// Create the message with all test data to be sent to Fenix
	testdataRowsMessages = &fenixTestDataSyncServerGrpcApi.TestdataRowsMessages{
		TestDataClientGuid:           common_config.FenicClientTestDataSyncServer_TestDataClientGuid,
		TestDataRows:                 testdataRows,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.SendTestDataRows(ctx, testdataRowsMessages)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "b457b233-41f9-4b3d-9f1e-00782b467045",
			"error": err,
		}).Fatal("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataRows'")

		// FenixTestDataSyncServer couldn't handle gPRC call
		if returnMessage.Acknack == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID": "c1f6a351-fb7e-4759-81a7-04ec61b74e59",
				"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
			}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataRows'")
		}
	}

}

// ********************************************************************************************************************
// Send the client's TestDataHeaders to Fenix by calling Fenix's gPRC server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) SendAreYouAliveToFenixTestDataServer() (bool, string) {

	// Set up connection to Server
	fenixClientTestDataSyncServerObject.SetConnectionToFenixTestDataSyncServer()

	// Create the message with all test data to be sent to Fenix
	emptyParameter := &fenixTestDataSyncServerGrpcApi.EmptyParameter{

		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	// Do gRPC-call
	ctx := context.Background()
	returnMessage, err := fenixTestDataSyncServerClient.AreYouAlive(ctx, emptyParameter)

	// Shouldn't happen
	if err != nil {
		fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
			"ID":    "818aaf0b-4112-4be4-97b9-21cc084c7b8b",
			"error": err,
		}).Fatal("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataRows'")

		// FenixTestDataSyncServer couldn't handle gPRC call
		if returnMessage.Acknack == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID": "2ecbc800-2fb6-4e88-858d-a421b61c5529",
				"Message from FenixTestDataSyncServerObject": returnMessage.Comments,
			}).Error("Problem to do gRPC-call to FenixTestDataSyncServer for 'SendTestDataRows'")
		}
	}

	return returnMessage.Acknack, returnMessage.Comments

}
