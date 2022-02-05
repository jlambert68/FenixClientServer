package main

import (
	"FenixClientServer/common_config"
	"github.com/go-gota/gota/dataframe"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"log"
	"os"
)

func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) CreateRowsMessage(merklePaths []string) (testdataRowsMessages *fenixTestDataSyncServerGrpcApi.TestdataRowsMessages) {

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

	numberOfColumnsToProcess := df.Ncol()
	numberOfRows := df.Nrow()
	for rowCounter := 0; rowCounter < numberOfRows; rowCounter++ {

		var valuesToHash []string
		var testdataItems []*fenixTestDataSyncServerGrpcApi.TestDataItemMessage

		for columnCounter := 0; columnCounter < numberOfColumnsToProcess; columnCounter++ {
			// add values for one row
			testDataItemValueAsString = df.Elem(rowCounter, columnCounter).String()
			testdataItemMessage = &fenixTestDataSyncServerGrpcApi.TestDataItemMessage{
				TestDataItemValueAsString: testDataItemValueAsString,
			}
			testdataItems = append(testdataItems, testdataItemMessage)
			valuesToHash = append(valuesToHash, testDataItemValueAsString)
		}

		// Hash all values for row
		hashedRow := common_config.HashValues(valuesToHash, true)

		// Create one row object and add it to array
		testDataRowMessage = &fenixTestDataSyncServerGrpcApi.TestDataRowMessage{
			RowHash:       hashedRow,
			LeafNodeName:  "XXXXX",
			LeafNodePath:  merkleFilterPath,
			TestDataItems: testdataItems,
		}
		testdataRows = append(testdataRows, testDataRowMessage)

	}

	// Get all Headers (No good solution, but it works)
	testDataHeaderMessage := fenixClientTestDataSyncServerObject.createTestDataHeaderMessage()
	var header *fenixTestDataSyncServerGrpcApi.TestDataItemHeaderLabelMessage
	var headers []*fenixTestDataSyncServerGrpcApi.TestDataItemHeaderLabelMessage

	for _, testDataHeader := range testDataHeaderMessage.TestDataHeaderItems {
		header = &fenixTestDataSyncServerGrpcApi.TestDataItemHeaderLabelMessage{
			TestDataItemHeaderLabel: testDataHeader.HeaderLabel}
		headers = append(headers, header)
	}

	testDataHeaderLabelsMessage := &fenixTestDataSyncServerGrpcApi.TestDataHeaderLabelsMessage{
		HeaderLabelsHash:         testDataHeaderMessage.HeaderLabelsHash,
		TestDataItemHeaderLabels: headers,
	}

	// Create the message with all test data to be sent to Fenix
	testdataRowsMessages = &fenixTestDataSyncServerGrpcApi.TestdataRowsMessages{
		TestDataClientUuid:           common_config.FenicClientTestDataSyncServer_TestDataClientUuid,
		TestDataHeaderLabels:         testDataHeaderLabelsMessage,
		TestDataRows:                 testdataRows,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	return testdataRowsMessages

}
