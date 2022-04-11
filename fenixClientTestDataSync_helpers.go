package main

import (
	"FenixClientServer/common_config"
	"fmt"
	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/api/idtoken"
	grpcMetadata "google.golang.org/grpc/metadata"
	"log"
	"os"
	"strings"
	"time"
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
		merkleFilterColumns := common_config.ExtractValuesFromFilterPath(fenixClientTestDataSyncServerObject.merkleFilterPath)

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
	merkleFilterPathFull := fenixClientTestDataSyncServerObject.merkleFilterPath
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
		TestDataClientUuid:           fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		TestDataHeaderLabels:         testDataHeaderLabelsMessage,
		TestDataRows:                 testdataRows,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
	}

	return testdataRowsMessages

}

/*
// // Convert cloudDBExposedTestDataRowItems into gRPC-RowsMessage
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) convertCloudDBExposedTestDataRowItemsgRpcToTestDataRowsMessage(
	exposedTestDataRowItems []cloudDBExposedTestDataRowItemsStruct) (gRpcTestDataRowsItemMessage *fenixTestDataSyncServerGrpcApi.TestdataRowsMessages) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "bdea5110-1af8-4e2f-a78a-ed1b2ad15514",
	}).Debug("Incoming gRPC 'convertCloudDBExposedTestDataRowItemsgRpcToTestDataRowsMessage'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "50c6be0a-a522-4aa6-ae7e-1db5936846f1",
	}).Debug("Outgoing gRPC 'convertCloudDBExposedTestDataRowItemsgRpcToTestDataRowsMessage'")


	var gRPCRowMessage fenixTestDataSyncServerGrpcApi.TestdataRowsMessages
	var gRPCTestDataRows fenixTestDataSyncServerGrpcApi.TestdataRowsMessages
	var gRPCTestDataHeaderLabels fenixTestDataSyncServerGrpcApi.TestDataHeaderLabelsMessage
	var gRPCTestDataItemHeaderLabels fenixTestDataSyncServerGrpcApi.TestDataItemHeaderLabelMessage
	var gRPCTestDataRowMessage fenixTestDataSyncServerGrpcApi.TestDataRowMessage
	var gRPCTestDataItems fenixTestDataSyncServerGrpcApi.TestDataItemMessage

	// General Message
	gRPCRowMessage = fenixTestDataSyncServerGrpcApi.TestdataRowsMessages{
		TestDataClientUuid:           fenixClientTestDataSyncServerObject.fenixClientTestDataSyncServer_TestDataClientUuid,
		ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),,
		TestDataHeaderLabels:         nil,
		TestDataRows:                 nil,
	}

	// Header Info
	gRPCTestDataHeaderLabels = fenixTestDataSyncServerGrpcApi.TestDataHeaderLabelsMessage{
		HeaderLabelsHash:         "",
		TestDataItemHeaderLabels: nil,
	}

	gRPCTestDataItemHeaderLabels =  fenixTestDataSyncServerGrpcApi.TestDataItemHeaderLabelMessage{
		TestDataItemHeaderLabel: "",
	}

	// TestDataRows Info
	gRPCTestDataRowMessage = fenixTestDataSyncServerGrpcApi.TestDataRowMessage{
		RowHash:       "",
		LeafNodeName:  "",
		LeafNodePath:  "",
		TestDataItems: nil,
	}

	gRPCTestDataItems = fenixTestDataSyncServerGrpcApi.TestDataItemMessage{
		TestDataItemValueAsString: "",
	}



	// Loop over all 'exposedTestDataRowItems' and convert into gRPC-message
	for _, exposedTestDataRowItem := range exposedTestDataRowItems {
		gRPCRowMessage = fenixTestDataSyncServerGrpcApi.TestdataRowsMessages{
			TestDataClientUuid:           "",
			ProtoFileVersionUsedByClient: fenixTestDataSyncServerGrpcApi.CurrentFenixTestDataProtoFileVersionEnum(fenixClientTestDataSyncServerObject.getHighestFenixProtoFileVersion()),
			TestDataHeaderLabels:         nil,
			TestDataRows:                 nil,
		}
	}

	gRpcTestDataRowsMessage := gRpcTestDataRowsItemMessage.TestDataRows
	// Loop over gRPC-TestDataRow-messages and convert into memoryDB-object
	for gRpcTestDataRow, gRpcTestDataRowMessage := range gRpcTestDataRowsMessage {



		// Loop over columns in 'testDataColumnsItem'
		testDataColumnsItem := gRpcTestDataRowMessage.TestDataItems
		for testDataColumn, columnValue := range testDataColumnsItem {

			// Extract data and populate memoryDB-object
			memDBtestDataRowItem := cloudDBTestDataRowItemCurrentStruct{
				clientUuid:            gRpcTestDataRowsItemMessage.TestDataClientUuid,
				rowHash:               gRpcTestDataRowMessage.RowHash,
				testdataValueAsString: columnValue.TestDataItemValueAsString,
				leafNodeName:          gRpcTestDataRowMessage.LeafNodeName,
				leafNodePath:          gRpcTestDataRowMessage.LeafNodePath,
				leafNodeHash:          "", //leafNodeHash,
				valueColumnOrder:      testDataColumn,
				valueRowOrder:         gRpcTestDataRow,
				updatedTimeStamp:      "",
			}

			// Add 'memDBtestDataRowItem' to array
			memDBtestDataRowItems = append(memDBtestDataRowItems, memDBtestDataRowItem)
		}
	}

	return memDBtestDataRowItems
}


*/
/*
// Convert TestDataRow message into TestData dataframe object
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) convertCloudDBTestDataRowItemsMessageToDataFrame(cloudDBTestDataRowItems []cloudDBTestDataRowItemCurrentStruct) (testdataAsDataFrame dataframe.DataFrame, returnMessage *fenixTestDataSyncServerGrpcApi.AckNackResponse) {

	fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "4a22eea7-806f-4d50-9b2f-1d5449203db6",
	}).Debug("Incoming gRPC 'convertCloudDBTestDataRowItemsMessageToDataFrame'")

	defer fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
		"id": "9768e8b2-0c0f-41cc-90e5-3f0c17bb9ed8",
	}).Debug("Outgoing gRPC 'convertCloudDBTestDataRowItemsMessageToDataFrame'")

	testdataAsDataFrame = dataframe.New()

	currentTestDataClientGuid := cloudDBTestDataRowItems[0].clientUuid

	currentTestDataHeaders := fenixClientTestDataSyncServerObject.getCurrentHeadersForClient(currentTestDataClientGuid)

	// If there are no headers in Database then Ask client for HeaderHash
	if len(currentTestDataHeaders) == 0 {
		fenixTestDataSyncServerObject.AskClientToSendTestDataHeaderHash(currentTestDataClientGuid)
		currentTestDataHeaders = fenixTestDataSyncServerObject.getCurrentHeadersForClient(currentTestDataClientGuid)

		// Validate that we got hte TestData Headers
		if len(currentTestDataHeaders) == 0 {

			// Set Error codes to return message
			var errorCodes []fenixTestDataSyncServerGrpcApi.ErrorCodesEnum
			var errorCode fenixTestDataSyncServerGrpcApi.ErrorCodesEnum

			errorCode = fenixTestDataSyncServerGrpcApi.ErrorCodesEnum_ERROR_UNKNOWN_CALLER //TODO Change to correct error
			errorCodes = append(errorCodes, errorCode)

			// Create Return message
			returnMessage = &fenixTestDataSyncServerGrpcApi.AckNackResponse{
				AckNack:    false,
				Comments:   "Fenix Asked for TestDataHeaders but didn't receive them i a correct way",
				ErrorCodes: errorCodes,
			}

			fenixTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"Id": "b20fb287-2e60-4f6f-b635-fea49f367a67",
			}).Info("Fenix Asked for TestDataHeaders but didn't receive them i a correct way")

			// leave
			return testdataAsDataFrame, returnMessage
		}
	}

	// Add 'KEY' to all headers
	var testDataHeadersInDataFrame []string
	testDataHeadersInDataFrame = append(testDataHeadersInDataFrame, currentTestDataHeaders...)
	testDataHeadersInDataFrame = append(testDataHeadersInDataFrame, "TestDataHash")

	//testDataRows := testdataRowsMessages.TestDataRows
	// Create matrix for testdata
	dataMatrix := make(map[int]map[int]string) //make(map[<row>>]map[<column>]<value>

	// Create a map for RowHashes
	testDataRowHashes := make(map[int]string) //make(map[<row>>]<rowHash>

	// Loop over all 'cloudDBTestDataRowItems' and add to matriix
	for _, cloudDBTestDataRowItem := range cloudDBTestDataRowItems {

		// Verify that datapoint doesn't exist
		_, dataPointExists := dataMatrix[cloudDBTestDataRowItem.valueRowOrder][cloudDBTestDataRowItem.valueColumnOrder]
		if dataPointExists == true {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"Id":                                   "efa873d1-b023-48db-b948-67fe00e103d7",
				"cloudDBTestDataRowItem.valueRowOrder": cloudDBTestDataRowItem.valueRowOrder,
				"cloudDBTestDataRowItem.valueColumnOrder": cloudDBTestDataRowItem.valueRowOrder,
			}).Fatal("Datapoint should only appears once")
		}

		// Add data to matrix
		// If 'row-map" already doesn't exist then initiate it
		_, rowExists := dataMatrix[cloudDBTestDataRowItem.valueRowOrder]
		if rowExists == false {
			// Initiate row in map and add column value
			dataMatrix[cloudDBTestDataRowItem.valueRowOrder] = map[int]string{}
			dataMatrix[cloudDBTestDataRowItem.valueRowOrder][cloudDBTestDataRowItem.valueColumnOrder] = cloudDBTestDataRowItem.testdataValueAsString
		} else {
			// Row exists then just add column value
			dataMatrix[cloudDBTestDataRowItem.valueRowOrder][cloudDBTestDataRowItem.valueColumnOrder] = cloudDBTestDataRowItem.testdataValueAsString
		}

		// Only add RowHash if it not exists
		_, rowHashExists := testDataRowHashes[cloudDBTestDataRowItem.valueRowOrder]
		if rowHashExists == false {
			testDataRowHashes[cloudDBTestDataRowItem.valueRowOrder] = cloudDBTestDataRowItem.rowHash
		}
	}

	// Loop all MerkleTreeNodes and create a DataFrame for the data
	numberOfRowsInMatrix := len(dataMatrix)
	var numberOfColumnsInMatrixRow int
	var numberOfColumnInFirstMatrixRow int

	for testDataRowCounter := 0; testDataRowCounter < numberOfRowsInMatrix; testDataRowCounter++ {

		// Extract row
		testDataRow := dataMatrix[testDataRowCounter]

		// Create one row, as a dataframe
		rowDataframe := dataframe.New()
		var valuesToHash []string

		// Get the number of columns in row
		numberOfColumnsInMatrixRow = len(testDataRow)

		// Verify that all rows have the same number of columns
		if testDataRowCounter == 0 {
			numberOfColumnInFirstMatrixRow = numberOfColumnsInMatrixRow

		} else {

			if numberOfColumnsInMatrixRow != numberOfColumnInFirstMatrixRow {
				fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
					"Id":                             "a2043be7-657a-4c94-a1a0-374243e82571",
					"numberOfColumnInFirstMatrixRow": numberOfColumnInFirstMatrixRow,
					"numberOfColumnsInMatrixRow":     numberOfColumnsInMatrixRow,
				}).Fatal("It seems that all TestDataRows doesn't have the same number o columns")
			}
		}

		// Loop over columns
		for testDataColumnCounter := 0; testDataColumnCounter < numberOfColumnsInMatrixRow; testDataColumnCounter++ {
			//		for testDataItemCounter, testDataItem := range testDataRow {

			if rowDataframe.Nrow() == 0 {
				// Create New
				rowDataframe = dataframe.New(
					series.New([]string{testDataRow[testDataColumnCounter]}, series.String, currentTestDataHeaders[testDataColumnCounter]))
			} else {
				// Add to existing
				rowDataframe = rowDataframe.Mutate(
					series.New([]string{testDataRow[testDataColumnCounter]}, series.String, currentTestDataHeaders[testDataColumnCounter]))
			}

			valuesToHash = append(valuesToHash, testDataRow[testDataColumnCounter])
		}

		// Create and add column for 'TestDataHash'
		testDataHashSeriesColumn := series.New([]string{"key"}, series.String, "TestDataHash")
		rowDataframe = rowDataframe.Mutate(testDataHashSeriesColumn)

		// Hash all values for row
		hashedRow := fenixSyncShared.HashValues(valuesToHash, true)

		// Validate that Row-hash is correct calculated
		if hashedRow != testDataRowHashes[testDataRowCounter] {

			// Set Error codes to return message
			var errorCodes []fenixTestDataSyncServerGrpcApi.ErrorCodesEnum
			var errorCode fenixTestDataSyncServerGrpcApi.ErrorCodesEnum

			errorCode = fenixTestDataSyncServerGrpcApi.ErrorCodesEnum_ERROR_ROWHASH_NOT_CORRECT_CALCULATED
			errorCodes = append(errorCodes, errorCode)

			// Create Return message
			returnMessage = &fenixTestDataSyncServerGrpcApi.AckNackResponse{
				AckNack:    false,
				Comments:   "RowsHashes seems not to be correct calculated.",
				ErrorCodes: errorCodes,
			}

			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"Id": "9e591230-1100-4771-ae38-c98a71daf784",
			}).Info("RowsHashes seems not to be correct calculated.")

			// Exit function Respond back to client when hash error
			return testdataAsDataFrame, returnMessage
		}

		// Add TestDataHash to row DataFrame
		rowDataframe.Elem(0, rowDataframe.Ncol()-1).Set(hashedRow)
		//) Mutate(
		//	series.New([]string{hashedRow}, series.String, "TestDataHash"))

		// Add the row to the Dataframe for the testdata
		// Special handling first when first time
		if testdataAsDataFrame.Nrow() == 0 {
			testdataAsDataFrame = rowDataframe.Copy()

		} else {
			testdataAsDataFrame = testdataAsDataFrame.OuterJoin(rowDataframe, testDataHeadersInDataFrame...)
		}
	}

	return testdataAsDataFrame, nil

}
*/

// Generate Google access token. Used when running in GCP
func (fenixClientTestDataSyncServerObject *fenixClientTestDataSyncServerObject_struct) generateGCPAccessToken(ctx context.Context) (appendedCtx context.Context, returnAckNack bool, returnMessage string) {

	// Only create the token if there is none, or it has expired
	if fenixClientTestDataSyncServerObject.gcpAccessToken == nil || fenixClientTestDataSyncServerObject.gcpAccessToken.Expiry.Before(time.Now()) {

		// Create an identity token.
		// With a global TokenSource tokens would be reused and auto-refreshed at need.
		// A given TokenSource is specific to the audience.
		tokenSource, err := idtoken.NewTokenSource(ctx, "https://"+common_config.FenixTestDataSyncServerAddress)
		if err != nil {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":  "8ba622d8-b4cd-46c7-9f81-d9ade2568eca",
				"err": err,
			}).Error("Couldn't generate access token")

			return nil, false, "Couldn't generate access token"
		}

		token, err := tokenSource.Token()
		if err != nil {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":  "0cf31da5-9e6b-41bc-96f1-6b78fb446194",
				"err": err,
			}).Error("Problem getting the token")

			return nil, false, "Problem getting the token"
		} else {
			fenixClientTestDataSyncServerObject.logger.WithFields(logrus.Fields{
				"ID":    "8b1ca089-0797-4ee6-bf9d-f9b06f606ae9",
				"token": token,
			}).Debug("Got Bearer Token")
		}

		fenixClientTestDataSyncServerObject.gcpAccessToken = token

	}

	// Add token to gRPC Request.
	appendedCtx = grpcMetadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+fenixClientTestDataSyncServerObject.gcpAccessToken.AccessToken)

	return appendedCtx, true, ""

}
