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
	"time"
	)

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
	url := flag.String ("url", "127.0.0.1:88888", "URL that will be used by the server")

	flag.Parse()

	/* We check whether the basedir is valid or not */
	_, myerror := os.Stat (*basedir)
	if (myerror != nil) { log.Fatal (myerror) }
	fmt.Println ("Basedir:", *basedir)

	/* Check the block size */
	fmt.Println ("Block size:", *block_size)
	if (*block_size == 0) { log.Fatal ("Invalid block size") }

	/* Check the URL */
	fmt.Println ("URL:", *url)

	/* From here, we know that we have all the required information to start the server */
	myserver := ds.ServerInit (*basedir, *block_size, *url)
	if (myserver == nil) { log.Fatal ("Cannot create server") }

	for {
		time.Sleep (1 * time.Second)
		if (ds.IsServerDone() == 1) { break }
	}

	fmt.Println ("All done. Bye")
}
