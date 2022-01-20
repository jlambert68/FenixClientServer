package main

import (
	"context"
	echo "github.com/jlambert68/FenixGrpcApi/Client/fenixClientTestDataSyncServerGrpcApi/echo/go_grpc_api"
)

func (s *ecServer) UnaryEcho(ctx context.Context, req *echo.EchoRequest) (*echo.EchoResponse, error) {
	return &echo.EchoResponse{Message: req.Message}, nil
}
