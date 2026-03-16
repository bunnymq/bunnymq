package data

import (
	coorddata "github.com/bunnymq/bunnymq/internal/coordinator/data"
	coordgroup "github.com/bunnymq/bunnymq/internal/coordinator/group"
)

// Re-export coordinator sentinel errors so mapDataError and test stubs share a
// single error identity across the api/data and coordinator/data packages.
var ErrOffsetNotFound = coorddata.ErrOffsetNotFound
var ErrOffsetOutOfRange = coorddata.ErrOffsetOutOfRange
var ErrStaleGeneration = coordgroup.ErrStaleGeneration
var ErrNotGroupMember = coordgroup.ErrNotGroupMember
