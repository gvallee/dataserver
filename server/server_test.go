/*
 * Copyright(c)		Geoffroy Vallee
 *			All rights reserved
 */

package server

import ("testing"
        "fmt"
	"log"
	"strconv"
	"os")

import err "github.com/gvallee/syserror"
import comm "github.com/gvallee/fscomm"

func TestServerCreate (t *testing.T) {
	fmt.Print ("Testing with an empty basedir... ")
	s1 := ServerInit ("", 0, "")
	if (s1 != nil) { log.Fatal ("Test with basedir=nil failed") }
	fmt.Println ("PASS")

	fmt.Print ("Testing with an invalid path for basedir... ");
	s2 := ServerInit ("/a/crazy/path/that/does/not/exist", 1, "127.0.0.1:55555")
	if (s2 != nil) { log.Fatal ("Test with basedir pointing to a not-existing directory failed") }
	fmt.Println ("PASS")

	validTestPath := "/tmp/ns_test/"
	// First make sure everything is clean for the test
	d, myerror := os.Open (validTestPath)
	if (myerror == nil) {
		// The directory already exist, we delete it
		defer d.Close()
		myerror = os.RemoveAll(validTestPath)
		if (myerror != nil) { log.Fatal ("Cannot remove the basedir before running the tests") }
	}
	myerror = os.MkdirAll (validTestPath, 0700)
	if (myerror != nil) { log.Fatal ("FATAL ERROR: cannot create directory for testing") }

	// Run the actual test with an invalid block size
	fmt.Print ("Testing with a valid basedir and valid block size... ")
	s3 := ServerInit (validTestPath, 0, "127.0.0.1:55544")
	if (s3 != nil) { log.Fatal ("FATAL ERROR: Test with invalid block size failed") }

	fmt.Println ("PASS")

	// Run the actual test with a valid configuration
	fmt.Print ("Testing with a valid configuration...") // (please wait until the server times out)... ")
	valid_url := "127.0.0.1:4455"
	s4 := ServerInit (validTestPath, 1024 * 1024, valid_url)
	if (s4 == nil) { log.Fatal ("Test with valid basedir failed") }

	basedir, mysyserror := GetBasedir (s4)
	if (mysyserror != err.NoErr || basedir != validTestPath) { log.Fatal ("FATAL ERROR: Cannot get the server's basedir") }

	blocksize, mysyserror2 := GetBlocksize (s4)
	if (mysyserror2 != err.NoErr || blocksize != 1024 * 1024) { log.Fatal ("FATAL ERROR: Cannot get block size") }

	// To properly terminate the server, we connect to the server and send a term message.
        // This is the correct way to interact with the server
	fmt.Println ("\tConnecting to server for termination...")
        conn, bs, myerr := comm.Connect2Server (valid_url)
        if (conn == nil || bs != 1024 * 1024 || myerr != err.NoErr) { log.Fatal ("ERROR: Cannot connect to the server") }

        senderr := comm.SendMsg (conn, comm.TERMMSG, nil)
	if (senderr != err.NoErr) { log.Fatal ("Cannot send termination message") }

	// Message successfully sent, we poll for the server termination and let things happen
	for {
		if (IsServerDone () == 1) { break }
	}

	// We clean up again
	fmt.Println ("\tAll done, cleaning...")
	myerror = os.RemoveAll(validTestPath)
	if (myerror != nil) { log.Fatal ("FATAL ERROR: cannot cleanup") }
	fmt.Println ("PASS")
}

func checkNamespaceDir (myserver *Server, name string) {
        basedir, syserror := GetBasedir (myserver)
        if (syserror != err.NoErr) {
                log.Fatal ("FATAL ERROR: Cannot get server's basedir")
        }
        basedir += name
        _, myerror := os.Stat (basedir)
        if (myerror != nil) {
                log.Fatal ("Expected namespace directory is missing");
        }
}

func writeTest (myserver *Server, ns string, id uint64, size uint64, offset uint64) (uint64, err.SysError) {
        // Convert size from MB to bytes
        size = size

        // Create an initialized buffer
        var c int = 0
        //c = 0
        buff := make ([]byte, size)
        var s uint64 = 0

        b := make ([]byte, 1) // Simplify our life for populating the buffer
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

func readTest (myserver *Server, ns string, id uint64, size uint64, offset uint64) (uint64, []byte, err.SysError) {
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
                if (myerror != nil) { log.Fatal ("FATAL ERROR: Cannot remove basedir required for testing") }
        }
        myerror = os.MkdirAll (validTestPath, 0700)
        if (myerror != nil) { log.Fatal ("FATAL ERROR: Cannot create the server's basedir") }

        // Create the data server
	valid_url := "127.0.0.1:8888"
        myserver := ServerInit (validTestPath, 1024 * 1024, valid_url) // 1MB block size
        if (myserver == nil) { log.Fatal ("FATAL ERROR: Cannot create data server") }
	fmt.Println ("YES!!!")

        // We check whether the directory is really there
        checkNamespaceDir (myserver, "default")
        fmt.Println ("PASS")

        fmt.Print ("Testing a custom namespace... ")
        ns2 := NamespaceInit ("my_namespace_2", myserver)
        if (ns2 == nil) { log.Fatal ("FATAL ERROR: Cannot create custom namespace") }

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
        if (ws != 1 * 1024 * 1024 || mysyserr != err.NoErr) { log.Fatal ("FATAL ERROR: Valid write failed - Wrote ", ws, " bytes - ", mysyserr.Error()) }
        fmt.Println ("PASS")

        // Checking the content with the equivalent read
        fmt.Print ("Testing a valid read ")
        rs, _, myreaderr := readTest (myserver,
                                      "my_namespace_2", // namespace
                                      0, // blockid
                                      1, // Read 1MB
                                      0) // Offset
        if (rs != 1 * 1024 * 1024 || mysyserr != err.NoErr) { log.Fatal ("FATAL ERROR: Valid read failed - Read ", rs, " bytes - ", myreaderr.Error()) }
        fmt.Println ("PASS")

        // Now testing data writing that should fail
        fmt.Print ("Testing an invalid write ")
        ws, mysyserr = writeTest (myserver,
                                 "my_namespace_2", // namespace
                                 0, // blockid
                                 1, // Write 1MB
                                 1024) // offset
        if (mysyserr != err.ErrDataOverflow) { log.Fatal ("FATAL ERROR: Test did not fail correctly") }
        fmt.Println ("PASS")

        // Now test data read that should fail
        fmt.Print ("Testing an invalid read ")
        rs, _, myreaderr = readTest (myserver,
                                     "my_namespace_2", // namespace
                                     0, // blockid
                                     1, //Read 1 MB
                                     512) // offset
        if (myreaderr != err.ErrDataOverflow) { log.Fatal ("FATAL ERROR: Test was supposed to fail but succeeded") }
        fmt.Println ("PASS")

	// To properly terminate the server, we connect to the server and send a term message.
        // This is the correct way to interact with the server
        fmt.Println ("\tConnecting to server for termination...")
        conn, blocksize, myerr := comm.Connect2Server (valid_url)
        if (conn == nil || blocksize != 1024 * 1024 || myerr != err.NoErr) { log.Fatal ("ERROR: Cannot connect to the server") }

        senderr := comm.SendMsg (conn, comm.TERMMSG, nil)
        if (senderr != err.NoErr) { log.Fatal ("Cannot send termination message") }

        // Message successfully sent, we poll for the server termination and let things happen
        for {
                if (IsServerDone () == 1) { break }
        }

}

