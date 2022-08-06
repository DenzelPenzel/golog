package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/denisschmidt/golog/api/v1"
	"github.com/stretchr/testify/require"
)

// We test that we can append a record to a segment, read back the same record,
// and eventually hit the configured max size for both the store and index.
func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")

	defer os.RemoveAll(dir)

	writeData := &api.Record{Value: []byte("Hello world")}

	c := Config{}

	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)

	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(writeData)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		readData, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, writeData.Value, readData.Value)
	}

	_, err = s.Append(writeData)
	require.Equal(t, io.EOF, err)

	// maxed index
	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(writeData.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// maxed store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
