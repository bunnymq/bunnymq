package bunnymqv1

import "testing"

var _ ManagementServiceServer = (*UnimplementedManagementServiceServer)(nil)
var _ DataServiceServer = (*UnimplementedDataServiceServer)(nil)

func TestManagementServiceDefinition(t *testing.T) {}
func TestDataServiceDefinition(t *testing.T)       {}
