package common_config

// Used as command flags when starting up to decide from where it was started
var StartUpType StartUpTypeType

type StartUpTypeType int

const (
	LocalhostNodocker StartUpTypeType = iota
	LocalhostDocker
	GCP
)

var StartUpTypeMapping = map[StartUpTypeType]string{
	LocalhostNodocker: "LOCALHOST_NODOCKER",
	LocalhostDocker:   "LOCALHOST_DOCKER",
	GCP:               "GCP",
}
