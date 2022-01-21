package common_config

import "github.com/sirupsen/logrus"

// gRPC-ports
const FenixTestDataSyncServer_address = "fenixtestdataserver-nwxrrpoxea-lz.a.run.app" //"127.0.0.1"
const FenixTestDataSyncServer_port = 443

const FenixClientTestDataSyncServer_address = "127.0.0.1"
const FenixClientTestDataSyncServer_initial_port = 5998

const FenicClientTestDataSyncServer_TestDataClientGuid = "45a217d1-55ed-4531-a801-779e566d75cb"
const FenicClientTestDataSyncServer_DomainGuid = "1a164df8-55a6-4a83-82d0-944d8ca52df7"
const FenicClientTestDataSyncServer_DomainName = "Finess"

// Logrus debug level

//const LoggingLevel = logrus.DebugLevel
//const LoggingLevel = logrus.InfoLevel
const LoggingLevel = logrus.DebugLevel // InfoLevel
