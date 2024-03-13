package nodestore

import "strconv"

/*
objectID is a unique identifier for an object in the nodestore. We identify
objects with 64 bit unsigned integers. In the object keys they are represented
in decimal form.

object IDs are derived from the version store. On startup the server reserves a
big range of them to use. Hence over multiple startups, big gaps in the object
IDs will appear.
*/

////////////////////////////////////////////////////////////////////////////////

type objectID uint64

func (id objectID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}
