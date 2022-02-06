package main

import (
	"FenixClientServer/common_config"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"log"
	"os"
)

// Filter out the rows that server requested, all rows if server didn't request specific rows
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) filterOutRequestedTestDataRows(merklePaths []string, testDataToWorkWith *dataframe.DataFrame) {

	// Only filter rows when there are MerklePaths to filter on
	if len(merklePaths) == 0 {
		return
	}

	// Extract all headers, to be used for joining dataframes
	headerKeys := testDataToWorkWith.Names()

	// Create an "Empty version of the TestData dataframe
	//localTestDataCopy := testDataToWorkWith.Copy().Subset(0)
	localTestDataCopy := testDataToWorkWith.Filter(
		dataframe.F{
			Colname:    headerKeys[0],
			Comparator: series.Eq,
			Comparando: -999,
		})

	// Loop all merklePaths
	for _, merklPath := range merklePaths {

		// Create a temporary working copy of the testdata to work with
		localTempTestDataCopy := testDataToWorkWith.Copy()

		// Add Column to be used as filter
		numberOfRows := localTempTestDataCopy.Nrow()

		localTempTestDataCopy = localTempTestDataCopy.Mutate(
			series.New(make([]bool, numberOfRows), series.Bool, "FilterColumn"))

		// Extract all 'columns' from merkleFilterPath
		merkleFilterColumns := common_config.ExtractValuesFromFilterPath(merkleFilterPath)

		// Extract values to filter on, sent by Fenix TestData server
		merkleFilterValues := common_config.ExtractValuesFromFilterPath(merklPath)

		fmt.Println(merkleFilterColumns)
		fmt.Println(merkleFilterValues)

		// Loop over the MerkleFilterValues and filter the TestData
		for filterValueCounter, filterValue := range merkleFilterValues {
			// Filter out the leaf nodes
			localTempTestDataCopy = localTempTestDataCopy.Filter(
				dataframe.F{
					Colname:    merkleFilterColumns[filterValueCounter],
					Comparator: series.Eq,
					Comparando: filterValue,
				})
		}

		// Add the Rows that were the resulter after filtering
		localTestDataCopy = localTestDataCopy.OuterJoin(localTempTestDataCopy, headerKeys...)

	}

	// Return the rows
	testDataToWorkWith = &localTestDataCopy

}

// Create the TestData rows to be sent to Fenix TestData Server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) createRowsMessage(merklePaths []string) (testdataRowsMessages *fenixTestDataSyncServerGrpcApi.TestdataRowsMessages) {

	var testdataRows []*fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var testDataRowMessage *fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var testdataItemMessage *fenixTestDataSyncServerGrpcApi.TestDataItemMessage

	var testDataItemValueAsString string

	// Load Testdata file
	irisCsv, err := os.Open(testFile)
	if err != nil {
		log.Fatal(err)
	}
	defer irisCsv.Close()

	df := dataframe.ReadCSV(irisCsv,
		dataframe.WithDelimiter(';'),
		dataframe.HasHeader(true))

	// Filter out to only have requested rows
	fenixClientTestDataSyncServerObject.filterOutRequestedTestDataRows(merklePaths, &df)

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
