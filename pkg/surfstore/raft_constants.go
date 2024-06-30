package surfstore

import (
	"fmt"
)

var ErrServerCrashedUnreachable = fmt.Errorf("server is crashed or unreachable")
var ErrServerCrashed = fmt.Errorf("server is crashed")
var ErrNotLeader = fmt.Errorf("server is not the leader")
var ErrMissingLeader = fmt.Errorf("no leader found")
