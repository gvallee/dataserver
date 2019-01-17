/*
 * Copyright(c)         Geoffroy Vallee
 *                      All rights reserved
 */

/*
 * The data server provides the following features:
 * - support namespaces
 * - write a data block
 * - read a data block
 * - interact with the meta-data server
 */

package main

import (
	"fmt"
	"flag"
	"os"
	"log"
	)

import ns "./namespace"
import ds "./server"
//import err "github.com/gvallee/syserror"

/**
 * Main function that is used to create a binary that can be used to instantiate a data
 * server. Most of the code is in packages, not here.
 */
func main() {
	/* Argument parsing */
	basedir := flag.String ("basedir", "", "Data server base directory")
	block_size := flag.Uint64 ("block-size", 1, "Block size in MB")

	flag.Parse()

	/* We check whether the basedir is valid or not */
	_, myerror := os.Stat (*basedir)
	if (myerror != nil) {
		log.Fatal (myerror)
	}
	fmt.Println ("Basedir:", *basedir)

	/* Check the block size */
	fmt.Println ("Block size", *block_size)


	/* From here, we know that we have all the required information to start the server */
	myserver := ds.Init (*basedir, *block_size)
	if (myserver == nil) {
		log.Fatal ("Cannot create server")
	}

	mydefaultnamespace := ns.Init ("default", myserver) // Always use the default namespace by default
	if (mydefaultnamespace == nil) {
		log.Fatal ("Cannot initialized the default namespace")
	}
}
