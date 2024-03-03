package tree_test

//import (
//	"errors"
//	"io"
//	"os"
//	"testing"
//	"time"
//
//	"github.com/foxglove/mcap/go/mcap"
//	"github.com/stretchr/testify/require"
//	"github.com/wkalt/dp3/nodestore"
//	"github.com/wkalt/dp3/storage"
//	"github.com/wkalt/dp3/tree"
//	"github.com/wkalt/dp3/util"
//)
//
//func TestTreeWriter(t *testing.T) {
//	store := storage.NewMemStore()
//	cache := util.NewLRU[nodestore.NodeID, nodestore.Node](1e6)
//	ns := nodestore.NewNodestore(store, cache)
//	tw, err := tree.NewWriter(
//		util.DateSeconds("2010-01-01"),
//		util.DateSeconds("2030-01-01"),
//		60*time.Second,
//		64,
//		ns,
//	)
//	require.NoError(t, err)
//	f, err := os.Open("/home/wyatt/data/bags/demo.mcap")
//	require.NoError(t, err)
//	defer f.Close()
//	lexer, err := mcap.NewLexer(f)
//	require.NoError(t, err)
//	for {
//		tokenType, token, err := lexer.Next(nil)
//		if errors.Is(err, io.EOF) {
//			require.NoError(t, tw.Close())
//			break
//		}
//		require.NoError(t, err)
//		switch tokenType {
//		case mcap.TokenMessage:
//			message, err := mcap.ParseMessage(token)
//			require.NoError(t, err)
//			err = tw.WriteMessage(message)
//			require.NoError(t, err)
//		case mcap.TokenSchema:
//			schema, err := mcap.ParseSchema(token)
//			require.NoError(t, err)
//			err = tw.WriteSchema(schema)
//			require.NoError(t, err)
//		case mcap.TokenChannel:
//			channel, err := mcap.ParseChannel(token)
//			require.NoError(t, err)
//			err = tw.WriteChannel(channel)
//			require.NoError(t, err)
//		}
//	}
//}
//
