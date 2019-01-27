/*
 * Copyright(c)         Geoffroy Vallee
 *                      All rights reserved
 */

package server

import ("os"
	"log"
	"strconv"
	"fmt")

import err "github.com/gvallee/syserror"
import comm "github.com/gvallee/fscomm"

type Server struct {
	basedir         string
	block_size      uint64
	info		*comm.ServerInfo
}

type Namespace struct {
        path string
}

var server_done int = 0

/* Functions specific to the implementation of servers */

func IsServerDone () int {
	return server_done
}

func runCommServer (server *Server) err.SysError {
	var errorStatus err.SysError = err.NoErr
	info := server.info

	mycommerr := comm.CreateServer (info)
	if (mycommerr != err.NoErr) { fmt.Println ("error creating comm server"); return mycommerr }

	conn, commerr := comm.GetConnFromInfo (info)
	if (conn == nil || commerr != err.NoErr) { return commerr }

	comm.HandleHandshake (conn)
	for server_done != 1 {
		msghdr, syserr := comm.GetHeader (conn)
		if (syserr != err.NoErr) { fmt.Println ("ERROR: Cannot get header"); return err.ErrFatal }

		if (msghdr == comm.TERMMSG) {
			server_done = 1
		} else if (msghdr == comm.DATAMSG) {
			fmt.Println ("Handling data message")
			// Recv the length of the namespace
			nslen, syserr := comm.RecvUint64 (conn)
			if (syserr != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}

			// Recv the namespace
			namespace, nserr := comm.RecvNamespace (conn, nslen)
			if (nserr != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}

			// Recv blockid
			blockid, berr := comm.RecvUint64 (conn)
			if (berr != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}

			// Recv offset
			offset, oerr := comm.RecvUint64 (conn)
			if (oerr != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}

			// Recv data size
			size, serr := comm.RecvUint64 (conn)
			if (serr != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}

			// Recv the actual data
			data, derr := comm.DoRecvData (conn, size)
			if (derr != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}

			// Actually save the data
			_, we := BlockWrite (server, namespace, blockid, offset, data)
			if (we != err.NoErr) {
				server_done = 1
				errorStatus = err.ErrFatal
			}
		} else if (msghdr == comm.READREQ) {
			fmt.Println ("Recv'd a READREQ")
			namespace, blockid, offset, size, recverr := comm.HandleReadReq (conn)
			if (recverr != err.NoErr) { server_done = 1; errorStatus = err.ErrFatal }

			fmt.Println ("Reading block...", blockid, offset, size)
			// Upon reception of a read req, we get the data and send it back
			rs, buff, readerr := BlockRead (server, namespace, blockid, offset, size)
			if (uint64(rs) != size || readerr != err.NoErr) { server_done = 1; errorStatus = err.ErrFatal }

			fmt.Println ("Sending read data...")
			senderr := comm.SendMsg (conn, comm.RDREPLY, buff)
			if (senderr != err.NoErr) { server_done = 1; errorStatus = err.ErrFatal }
		} else {
			fmt.Println ("Unexpected message, terminating: ", msghdr)
			errorStatus = err.ErrFatal
		}

		if (server_done == 1) { fmt.Println ("All done:", errorStatus.Error()) }
	}

	fmt.Println ("Finalizing server...")
	comm.FiniServer ()

	return errorStatus
}

/**
 * Initialize the data server
 * @param[in]	basedir	Path to the basedir directory that the server must use
 * @param[in]	block_size	Cannonical size of a block
 * @return	Pointer to a new Server structure; nil if error
 */
func ServerInit (basedir string, block_size uint64, server_url string) *Server {
	// Deal with the server's basedir (we have to make sure it exists)
	_, myerror := os.Stat (basedir)
	if (myerror != nil) { return nil }

	// Check whether the block size is valid
	if (block_size == 0) { return nil }
	block_size = block_size

	// Create and return the data structure for the new server
	new_server := new (Server)
	new_server.basedir = basedir
	new_server.block_size = block_size
	new_server.info = comm.CreateServerInfo (server_url, block_size, 60)

	// Initialize the default namespace
	mydefaultnamespace := NamespaceInit ("default", new_server) // Always use the default namespace by default
	if (mydefaultnamespace == nil) { fmt.Println ("Cannot initialized the default namespace"); return nil }

	go runCommServer (new_server)

	return new_server
}

func ServerFini () {
	// TODO close all file descriptors associated to blocks
}

/**
 * Return the basedir of the data server
 * @param[in]	ds	Structure representing the server
 * @return	String specifying the server's basedir path
 * @return	System error handle
 */
func GetBasedir (ds *Server) (string, err.SysError) {
	if (ds == nil) {
		return err.ErrNotAvailable.Error(), err.ErrNotAvailable
	}
	return ds.basedir, err.NoErr
}

/**
 * Return the block size of the data server.
 * This is a server level parameter, not a namespace level parameters, at least not
 * at the moment.
 * @param[in]	ds	Structure representing the server
 * @return	Size of the block
 * @return	System error handle
 */
func GetBlocksize (ds *Server) (uint64, err.SysError) {
	if (ds == nil) { return 0, err.ErrNotAvailable }

	return ds.block_size, err.NoErr
}

/* Functions specific to the implementation of namespaces */

/**
 * Initialize a namespace. The function can safely be called multiple times. If the
 * namespace already exists, the function simply returns successfully.
 * @param[in]   name    Namespace's name
 * @param[in]   ds      Structure representing the server
 * @return      Namespace handle
 */
func NamespaceInit (name string, dataserver *Server) *Namespace {
        namespacePath, myerr := GetBasedir (dataserver)
        if (myerr != err.NoErr) {
                fmt.Println (myerr.Error())
                return nil
        }
        namespacePath += "/"
        namespacePath += name
        _, myerror := os.Stat (namespacePath)
        if (myerror != nil) {
                // The path does not exist
                myerror := os.MkdirAll (namespacePath, 0700)
                if (myerror != nil) {
                        log.Fatal (myerror)
                        return nil
                }
        }

        new_namespace := new (Namespace)
        new_namespace.path = namespacePath
        return new_namespace
}

/**
 * Get the path to the file where the block is saved. The underlying file will be correctly
 * opened/created.
 * @param[in]   ds      Structure representing the server
 * @param[in]   namespace       Namespace's namespace we want to write to
 * @param[in]   blockid         Block id to write to
 * @return      File handle that can be used for write operations
 */
func getBlockPath (dataserver *Server, namespace string, blockid uint64) (*os.File, string, err.SysError) {
        block_file, myerr := GetBasedir (dataserver)
        if (myerr != err.NoErr) {
                fmt.Println (myerr.Error())
                return nil, "", myerr
        }
        block_file += namespace + "/block"
        block_file += strconv.FormatUint (blockid, 10)

	/*
        _, mystaterror := os.Stat (block_file)

        // Perform the actual write operation
        var f *os.File
        var myerror error
        if (mystaterror != nil) {
                // The file does not exist yet
		fmt.Println ("Creating block file")
                f, myerror = os.Create (block_file)
        } else {
                // The file exists, we open it
		fmt.Println ("Opening block file")
                f, myerror = os.OpenFile (block_filei, os.O_RDWR|os.O_CREATE, 0755))
        }
	*/
	f, myerror := os.OpenFile (block_file, os.O_RDWR|os.O_CREATE, 0755)
        if (myerror != nil) {
                fmt.Println (myerror.Error())
                return nil, "", err.ErrNotAvailable
        }

        return f, block_file, err.NoErr
}

/**
 * Write a data to a block 
 * @param[in]   ds      Structure representing the server
 * @param[in]   namespace       Namespace's namespace we want to write to
 * @param[in]   blockid         Block id to write to
 * @param[in]   offset          Write offset
 * @param[in]   data            Buffer with the data to write to the block
 * @return      Amount of data written to the block in bytes
 * @return      System error handle
 */
func BlockWrite (dataserver *Server, namespace string, blockid uint64, offset uint64, data []byte) (int, err.SysError) {
        // Making sure that the data to write fits into the block
        blocksize, dserr := GetBlocksize (dataserver)
        if (dserr != err.NoErr) { fmt.Println (dserr.Error()); return -1, dserr }
        if (offset + uint64 (len (data)) > blocksize) {
		fmt.Println ("Data overflow - Write", len(data), "from", offset, "while blocksize is", blocksize)
                return -1, err.ErrDataOverflow
        }

        // Figure out where to write the data
        f, _, myerr := getBlockPath (dataserver, namespace, blockid)
	defer f.Close()
        if (myerr != err.NoErr) {
                return -1, myerr
        }

        // Actually write the data
	fmt.Println ("Actually writing", len (data), "bytes to block", blockid, ", starting at", offset)
        s, mywriteerr := f.WriteAt (data, int64(offset)) // Unfortunately, Write return an INT
        if (mywriteerr != nil) {
		fmt.Println (mywriteerr.Error())
                return -1, err.ErrFatal
        }
        f.Sync()

        // All done
        return s, err.NoErr
}

/**
 * Read a data block 
 * @param[in]   ds      Structure representing the server
 * @param[in]   namespace       Namespace's namespace we want to write to
 * @param[in]   blockid         Block id to write to
 * @param[in]   offset          Write offset
 * @param[in]   size            Amount of data to read
 * @return      Amount of data written to the block in bytes
 * @return      Buffer with the data read from the block
 * @return      System error handle
 */
func BlockRead (dataserver *Server, namespace string, blockid uint64, offset uint64, size uint64) (int, []byte, err.SysError) {
        blocksize, dserr := GetBlocksize (dataserver)
        if (dserr != err.NoErr) {
                return -1, nil, err.ErrFatal
        }
        if (offset + size > blocksize) {
                return -1, nil, err.ErrDataOverflow
        }

        // Figure out from where to read the data
        f, _, myerr := getBlockPath (dataserver, namespace, blockid)
	defer f.Close()
        if (myerr != err.NoErr) {
                fmt.Println (myerr.Error())
                return -1, nil, myerr
        }

        // Actually read the data
        buff := make ([]byte, size)
	fmt.Println ("Actually reading", size, " bytes from block", blockid, ", starting at", offset)
        s, myreaderr := f.ReadAt (buff, int64 (offset)) // Unfortunately Read return an INT
        if (myreaderr != nil) {
                fmt.Println ("ERRROR: Cannot read from file")
                return -1, nil, err.ErrFatal
        }

        // All done
        return s, buff, err.NoErr
}

