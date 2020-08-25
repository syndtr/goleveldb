package leveldb

import (
	"testing"

	"github.com/go-leveldb/goleveldb/leveldb/testutil"
)

func TestLevelDB(t *testing.T) {
	testutil.RunSuite(t, "LevelDB Suite")
}
