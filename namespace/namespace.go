package namespace

import ("os"
	"log"
	"strconv"
	"fmt"
       )

import ds "../server"

import err "github.com/gvallee/syserror"

type Namespace struct {
	path string
}

/**
 * Initialize a namespace. The function can safely be called multiple times. If the
 * namespace already exists, the function simply returns successfully.
 * @param[in]	name	Namespace's name
 * @param[in]	ds	Structure representing the server
 * @return	Namespace handle
 */
func Init (name string, dataserver *ds.Server) *Namespace {
	namespacePath, myerr := ds.GetBasedir (dataserver)
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
 * @param[in]	ds	Structure representing the server
 * @param[in]	namespace	Namespace's namespace we want to write to
 * @param[in]	blockid		Block id to write to
 * @return	File handle that can be used for write operations
 */
func getBlockPath (dataserver *ds.Server, namespace string, blockid uint64) (*os.File, string, err.SysError) {
	block_file, myerr := ds.GetBasedir (dataserver)
	if (myerr != err.NoErr) {
		fmt.Println (myerr.Error())
		return nil, "", myerr
	}
	block_file += namespace + "/block"
	block_file += strconv.FormatUint (blockid, 10)

	_, mystaterror := os.Stat (block_file)

	// Perform the actual write operation
	var f *os.File
	var myerror error
	if (mystaterror != nil) {
		// The file does not exist yet
		f, myerror = os.Create (block_file)
	} else {
		// The file exists, we open it
		f, myerror = os.Open (block_file)
	}
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
 * @param[in]	offset		Write offset
 * @param[in]	data		Buffer with the data to write to the block
 * @return      Amount of data written to the block in bytes
 * @return	System error handle
 */
func BlockWrite (dataserver *ds.Server, namespace string, blockid uint64, offset uint64, data []byte) (int, err.SysError) {
	// Making sure that the data to write fits into the block
	blocksize, dserr := ds.GetBlocksize (dataserver)
	if (dserr != err.NoErr) {
		return -1, err.ErrFatal
	}
	if (offset + uint64 (len (data)) > blocksize) {
		return -1, err.ErrDataOverflow
	}

	// Figure out where to write the data
	f, _, myerr := getBlockPath (dataserver, namespace, blockid)
	if (myerr != err.NoErr) {
		return -1, myerr
	}
	defer f.Close ()

	// Actually write the data
	s, mywriteerr := f.WriteAt (data, int64(offset)) // Unfortunately, Write return an INT
	if (mywriteerr != nil) {
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
 * @param[in]	size		Amount of data to read
 * @return      Amount of data written to the block in bytes
 * @return	Buffer with the data read from the block
 * @return      System error handle
 */
func BlockRead (dataserver *ds.Server, namespace string, blockid uint64, offset uint64, size uint64) (int, []byte, err.SysError) {
	blocksize, dserr := ds.GetBlocksize (dataserver)
	if (dserr != err.NoErr) {
		return -1, nil, err.ErrFatal
	}
	if (offset + size > blocksize) {
		return -1, nil, err.ErrDataOverflow
	}

	// Figure out from where to read the data
	f, _, myerr := getBlockPath (dataserver, namespace, blockid)
	if (myerr != err.NoErr) {
		fmt.Println (myerr.Error())
		return -1, nil, myerr
	}
	defer f.Close ()

	// Actually read the data
	buff := make ([]byte, size)
	s, myreaderr := f.ReadAt (buff, int64 (offset)) // Unfortunately Read return an INT
	if (myreaderr != nil) {
		fmt.Println ("ERRROR: Cannot read from file")
		return -1, nil, err.ErrFatal
	}

	// All done
	return s, buff, err.NoErr
}
