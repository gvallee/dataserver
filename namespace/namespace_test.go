/*
 * Copyright(c)         Geoffroy Vallee
 *                      All rights reserved
 */

package namespace

import ("testing"
	"log"
	"fmt"
	"os"
	"strconv"
      )

import ds "../server"
import err "github.com/gvallee/syserror"


func checkNamespaceDir (myserver *ds.Server, name string) {
	basedir, syserror := ds.GetBasedir (myserver)
	if (syserror != err.NoErr) {
		log.Fatal ("FATAL ERROR: Cannot get server's basedir")
	}
	basedir += name
	_, myerror := os.Stat (basedir)
	if (myerror != nil) {
		log.Fatal ("Expected namespace directory is missing");
	}
}

func writeTest (myserver *ds.Server, ns string, id uint64, size uint64, offset uint64) (uint64, err.SysError) {
	// Convert size from MB to bytes
	size = size * 1024 * 1024

	// Create an initialized buffer
	var c int = 0
	//c = 0
	buff := make ([]byte, size)
	var s uint64 = 0

	b := make ([]byte, 1) // Simply our life for populating the buffer
	i := 0
	for s < size {
		b[0] = byte (c)
		// Do we space to actually write the new value?
		if (s + uint64(len (b)) > size) {
			break
		}

		buff[i] = b[0]
		if (c == 9) {
			c = 0
		} else {
			c += 1
		}

		s += strconv.IntSize
		i += 1
	}

	ws, mysyserr := BlockWrite (myserver, ns, id, offset, buff)

	return uint64 (ws), mysyserr
}

func readTest (myserver *ds.Server, ns string, id uint64, size uint64, offset uint64) (uint64, []byte, err.SysError) {
	// Convert size from MB to B
	size = size * 1024 * 1024

	rs, buff,  mysyserr := BlockRead (myserver, ns, id, offset, size)

	// If we read from the begining of the file, we know what to expect and check
	// the content of the buffer
	if (offset == 0) {
		var c int = 0
		var s uint64 = 0
		var content int
		i := 0

		for s < size {
			content = int (buff[i])
			if (content != c) {
				fmt.Println ("ERROR: Got the wrong value ", content , " vs ", c, " pos: ", i)
				break
			}

			if (c == 9) {
				c = 0
			} else {
				c += 1
			}

			i += 1
			s += strconv.IntSize
		}
	}

	return uint64 (rs), buff, mysyserr
}

func TestNamespaceCreation (t *testing.T) {
	validTestPath := "/tmp/ns_test/"
	// First make sure everything is clean for the test
	d, myerror := os.Open (validTestPath)
	if (myerror == nil) {
		// The directory already exist, we delete it
		defer d.Close()
		myerror = os.RemoveAll(validTestPath)
		if (myerror != nil) {
			log.Fatal ("FATAL ERROR: Cannot remove basedir required for testing")
		}
	}
	myerror = os.MkdirAll (validTestPath, 0700)
	if (myerror != nil) {
		log.Fatal ("FATAL ERROR: Cannot create the server's basedir")
	}

	// Create the data server
	myserver := ds.Init (validTestPath, 1) // 1MB block size
	if (myserver == nil) {
		log.Fatal ("FATAL ERROR: Cannot create data server")
	}

	// Now that the data server is up, we can start the actual test
	fmt.Print ("Testing the default namespace... ")
	ns1 := Init ("default", myserver)
	if (ns1 == nil) {
		log.Fatal ("FATAL ERROR: Cannot create default namespace")
	}
	// We check whether the directory is really there
	checkNamespaceDir (myserver, "default")
	fmt.Println ("PASS")

	fmt.Print ("Testing a custom namespace... ")
	ns2 := Init ("my_namespace_2", myserver)
	if (ns2 == nil) {
		log.Fatal ("FATAL ERROR: Cannot create custom namespace")
	}
	// We check whether the directory is really there
	checkNamespaceDir (myserver, "my_namespace_2")
	fmt.Println ("PASS")

	// Now testing data writing that should succeed
	fmt.Print ("Testing a valid write ")
	ws, mysyserr := writeTest (myserver,
				  "my_namespace_2", // namespace
				  0, // blockid
				  1, // Write 1MB
				  0) // Offset
	if (ws != 1 * 1024 * 1024 || mysyserr != err.NoErr) {
		log.Fatal ("FATAL ERROR: Valid write failed - Wrote ", ws, " bytes - ", mysyserr.Error())
	}
	fmt.Println ("PASS")

	// Checking the content with the equivalent read
	fmt.Print ("Testing a valid read ")
	rs, _, myreaderr := readTest (myserver,
	                              "my_namespace_2", // namespace
	                              0, // blockid
	                              1, // Read 1MB
	                              0) // Offset
	if (rs != 1 * 1024 * 1024 || mysyserr != err.NoErr) {
		log.Fatal ("FATAL ERROR: Valid read failed - Read ", rs, " bytes - ", myreaderr.Error())
	}
	fmt.Println ("PASS")

	// Now testing data writing that should fail
	fmt.Print ("Testing an invalid write ")
	ws, mysyserr = writeTest (myserver,
				 "my_namespace_2", // namespace
				 0, // blockid
				 1, // Write 1MB
				 1024) // offset
	if (mysyserr != err.ErrDataOverflow) {
		log.Fatal ("FATAL ERROR: Test did not fail correctly")
	}
	fmt.Println ("PASS")

	// Now test data read that should fail
	fmt.Print ("Testing an invalid read ")
	rs, _, myreaderr = readTest (myserver,
	                             "my_namespace_2", // namespace
	                             0, // blockid
	                             1, //Read 1 MB
	                             512) // offset
	if (myreaderr != err.ErrDataOverflow) {
		log.Fatal ("FATAL ERROR: Test was supposed to fail but succeeded")
	}
	fmt.Println ("PASS")
}
