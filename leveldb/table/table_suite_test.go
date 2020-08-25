package table

import (
	"testing"

	"github.com/go-leveldb/goleveldb/leveldb/testutil"
)

func TestTable(t *testing.T) {
	testutil.RunSuite(t, "Table Suite")
}
