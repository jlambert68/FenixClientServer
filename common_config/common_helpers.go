package common_config

import (
	fenixTestDataSyncServerGrpcApi "github.com/jlambert68/FenixGrpcApi/Fenix/fenixTestDataSyncServerGrpcApi/go_grpc_api"
)

// Exctract Values, and create, for TestDataHeaderItemMessageHash
func CreateTestDataHeaderItemMessageHash(testDataHeaderItemMessage *fenixTestDataSyncServerGrpcApi.TestDataHeaderItemMessage) (testDataHeaderItemMessageHash string) {

	var valuesToHash []string
	var valueToHash string

	// Extract and add values to array
	// HeaderPresentationsLabel
	valueToHash = testDataHeaderItemMessage.HeaderPresentationsLabel
	valuesToHash = append(valuesToHash, valueToHash)

	// HeaderDataLabel
	valueToHash = testDataHeaderItemMessage.HeaderDataLabel
	valuesToHash = append(valuesToHash, valueToHash)

	// HeaderShouldBeUsedForTestDataFilter as 'true' or 'false'
	if testDataHeaderItemMessage.HeaderShouldBeUsedForTestDataFilter == false {
		valuesToHash = append(valuesToHash, "false")
	} else {
		valuesToHash = append(valuesToHash, "true")
	}

	// HeaderIsMandatoryInTestDataFilter as 'true' or 'false'
	if testDataHeaderItemMessage.HeaderIsMandatoryInTestDataFilter == false {
		valuesToHash = append(valuesToHash, "false")
	} else {
		valuesToHash = append(valuesToHash, "true")
	}

	// HeaderSelectionType
	valueToHash = testDataHeaderItemMessage.HeaderSelectionType.String()
	valuesToHash = append(valuesToHash, valueToHash)

	// HeaderFilterValues - An array thar is added
	valueToHash = testDataHeaderItemMessage.HeaderPresentationsLabel
	valuesToHash = append(valuesToHash, valueToHash)

	// Hash all values in the array
	testDataHeaderItemMessageHash = HashValues(valuesToHash)

	return testDataHeaderItemMessageHash
}
