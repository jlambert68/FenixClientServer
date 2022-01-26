package common_config

import "github.com/sirupsen/logrus"

// Addresses
//Const FenixTestDataSyncServerAddressGCP = "fenixtestdataserver-nwxrrpoxea-lz.a.run.app"
//Const FenixTestDataSyncServerAddressLocal = "92.168.2.93"

// gRPC-ports
const FenixTestDataSyncServer_address = "192.168.2.93" //"127.0.0.1" //"fenixtestdataserver-nwxrrpoxea-lz.a.run.app" //"127.0.0.1"//
const FenixTestDataSyncServer_port = 6660              //443

const FenixClientTestDataSyncServer_address = "127.0.0.1"
const FenixClientTestDataSyncServer_initial_port = 5998

const FenicClientTestDataSyncServer_TestDataClientUuid = "45a217d1-55ed-4531-a801-779e566d75cb"
const FenicClientTestDataSyncServer_DomainUuid = "1a164df8-55a6-4a83-82d0-944d8ca52df7"
const FenicClientTestDataSyncServer_DomainName = "Finess"

// Logrus debug level

//const LoggingLevel = logrus.DebugLevel
//const LoggingLevel = logrus.InfoLevel
const LoggingLevel = logrus.DebugLevel // InfoLevel
