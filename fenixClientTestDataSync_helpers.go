package main

import (
	"FenixClientServer/common_config"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"strings"
)

// Filter out the rows that server requested, all rows if server didn't request specific rows
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) filterOutRequestedTestDataRows(merkleTreeNodeNames []string, testDataToWorkWith *dataframe.DataFrame) {

	//TODO change this to do filtering on MerkleTreeNodeNames instead of MerkleFilterPath
	// THis is not used at the moment

	// Only filter rows when there are MerklePaths to filter on
	if len(merkleTreeNodeNames) == 0 {
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

	// Loop all merkleTreeNodeNames
	for _, merklPath := range merkleTreeNodeNames {

		// Create a temporary working copy of the testdata to work with
		localTempTestDataCopy := testDataToWorkWith.Copy()

		// Add Column to be used as filter
		//numberOfRows := localTempTestDataCopy.Nrow()

		//localTempTestDataCopy = localTempTestDataCopy.Mutate(
		//	series.New(make([]bool, numberOfRows), series.Bool, "FilterColumn"))

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

		// Add the Rows that were the result after filtering
		localTestDataCopy = localTestDataCopy.OuterJoin(localTempTestDataCopy, headerKeys...)

	}

	// Return the rows
	*testDataToWorkWith = localTestDataCopy

}

// Create the TestData rows to be sent to Fenix TestData Server
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) createRowsMessage(merkleTreeNodeNames []string) (testdataRowsMessages *fenixTestDataSyncServerGrpcApi.TestdataRowsMessages) {

	var testdataRows []*fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var testDataRowMessage *fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var testdataItemMessage *fenixTestDataSyncServerGrpcApi.TestDataItemMessage
	var nodeNameInServersRequestedList bool

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

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "737745ca-8b5b-4b35-9943-19b036cdb5a6",
	}).Debug("Read file: ", testFile)

	merkleHash, _, _ := common_config.LoadAndProcessFile(testFile)

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "ba09fb6a-52e1-4d26-8e71-3e600f4460eb",
	}).Debug("MerkleHash for Read file: ", merkleHash)

	// Extract each FilterPathValues into an array
	var merkleFilterValues []string
	merkleFilterPathFull := "AccountEnvironment/ClientJuristictionCountryCode/MarketSubType/MarketName/" //TODO use same source
	merkleFilterPath := merkleFilterPathFull

	startPosition := 0

	for {
		endPosition := strings.Index(merkleFilterPath, "/")

		// If no more '/' then exit for loop
		if endPosition == -1 {
			break
		}

		merklePathValue := merkleFilterPath[startPosition:endPosition]
		merkleFilterValues = append(merkleFilterValues, merklePathValue)
		merkleFilterPath = merkleFilterPath[endPosition+1:]

	}

	// List all Headers
	headerNames := df.Names()

	// Create Map over 'merkleFilterValue' -> Column-number
	merkleFilterValueToColumnNumberMap := make(map[string]int) //map[<merkleFilterValue>]=<Column number>

	// Loop over MerkleFilterValues and create map
	for _, merkleFilterValue := range merkleFilterValues {

		// Verify that value doesn't exist
		_, merkleFilterValueExists := merkleFilterValueToColumnNumberMap[merkleFilterValue]

		if merkleFilterValueExists == true {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":                "56822d4e-f9d8-4591-81fa-5af3f43867ed",
				"merkleFilterValue": merkleFilterValue,
			}).Fatal("'merkleFilterValue' already exists in map. This should not happen ")
		}

		// Loop over Headers to get column number for header, to create the map([merkleFilterValue]=HeaderColumnNumber)
		var headerFound = false
		for headerColumnNumber, headerName := range headerNames {

			if headerName == merkleFilterValue {
				merkleFilterValueToColumnNumberMap[merkleFilterValue] = headerColumnNumber
				headerFound = true
				break
			}
		}

		// If 'merkleFilterValue' wasn't found among Headers then there is some fishy stuff going on
		if headerFound == false {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":                "cdb5648a-8d8d-4d4e-9650-b3864b2bd34a",
				"merkleFilterValue": merkleFilterValue,
				"headerNames":       headerNames,
			}).Fatal("'merkleFilterValue' was not found among headers. This should not happen ")
		}
	}

	// Filter out to only have requested rows
	// Can't do that here because NodeName has not been added
	//fenixClientTestDataSyncServerObject.filterOutRequestedTestDataRows(merkleTreeNodeNames, &df)

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

		// Create the LeafNodeName
		var leafNodeName = "MerkleRoot/"
		for _, columnName := range merkleFilterValues {

			columnNumber := merkleFilterValueToColumnNumberMap[columnName]
			leafNodeNamePart := df.Elem(rowCounter, columnNumber).String()
			leafNodeName = leafNodeName + leafNodeNamePart + "/"
		}

		// Verify that NodeName is in List from Server, when list from Server contains anny NodeNames
		if len(merkleTreeNodeNames) > 0 {
			nodeNameInServersRequestedList = false
			for _, nodeName := range merkleTreeNodeNames {
				if nodeName == leafNodeName {
					nodeNameInServersRequestedList = true
				}
			}
		} else {
			nodeNameInServersRequestedList = true
		}

		// Only add testRowMessage when NodeName is requested by server
		if nodeNameInServersRequestedList == true {

			// Create one row object and add it to array
			testDataRowMessage = &fenixTestDataSyncServerGrpcApi.TestDataRowMessage{
				RowHash:       hashedRow,
				LeafNodeName:  leafNodeName,
				LeafNodePath:  merkleFilterPathFull,
				TestDataItems: testdataItems,
			}
			testdataRows = append(testdataRows, testDataRowMessage)
		}

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
