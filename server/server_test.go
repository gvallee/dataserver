/*
 * Copyright(c)		Geoffroy Vallee
 *			All rights reserved
 */

package server

import ("testing"
        "fmt"
	"log"
	"os")

import err "github.com/gvallee/syserror"

func TestServerCreate (t *testing.T) {
	fmt.Print ("Testing with an empty basedir... ")
	s1 := Init ("", 0)
	if (s1 != nil) {
		log.Fatal ("Test with basedir=nil failed")
	}
	fmt.Println ("PASS")

	fmt.Print ("Testing with an invalid path for basedir... ");
	s2 := Init ("/a/crazy/path/that/does/not/exist", 1)
	if (s2 != nil) {
		log.Fatal ("Test with basedir pointing to a not-existing directory failed")
	}
	fmt.Println ("PASS")

	validTestPath := "/tmp/ns_test/"
	// First make sure everything is clean for the test
	d, myerror := os.Open (validTestPath)
	if (myerror == nil) {
		// The directory already exist, we delete it
		defer d.Close()
		myerror = os.RemoveAll(validTestPath)
		if (myerror != nil) {
			log.Fatal ("Cannot remove the basedir before running the tests")
		}
	}
	myerror = os.MkdirAll (validTestPath, 0700)
	if (myerror != nil) {
		log.Fatal ("FATAL ERROR: cannot create directory for testing")
	}

	// Run the actual test with an invalid block size
	fmt.Print ("Testing with a valid basedir and valid block size... ")
	s3 := Init (validTestPath, 0)
	if (s3 != nil) {
		log.Fatal ("FATAL ERROR: Test with invalid block size failed")
	}
	fmt.Println ("PASS")

	// Run the actual test with a valid configuration
	fmt.Print ("Testing with a valid basedir and valid block size... ")
	s4 := Init (validTestPath, 1)
	if (s4 == nil) {
		log.Fatal ("Test with valid basedir failed");
	}
	basedir, mysyserror := GetBasedir (s4)
	if (mysyserror != err.NoErr || basedir != validTestPath) {
		log.Fatal ("FATAL ERROR: Cannot get the server's basedir")
	}
	blocksize, mysyserror2 := GetBlocksize (s4)
	if (mysyserror2 != err.NoErr || blocksize != 1 * 1024 * 1024) {
		log.Fatal ("FATAL ERROR: Cannot get block size")
	}

	// We clean up again
	myerror = os.RemoveAll(validTestPath)
	if (myerror != nil) {
		fmt.Println ("FATAL ERROR: cannot cleanup")
	}
	fmt.Println ("PASS")
}
