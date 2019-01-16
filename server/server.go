package server

import ("os")

import err "github.com/gvallee/syserror"

type Server struct {
	basedir string
	block_size uint64
}

/**
 * Initialize the data server
 * @param[in]	basedir	Path to the basedir directory that the server must use
 * @param[in]	block_size	Cannonical size of a block
 * @return	Pointer to a new Server structure; nil if error
 */
func Init (basedir string, block_size uint64) *Server {
	// Deal with the server's basedir (we have to make sure it exists)
	_, myerror := os.Stat (basedir)
	if (myerror != nil) {
		return nil
	}

	// Check whether the block size is valid and convert from MB to B
	if (block_size == 0) {
		return nil
	}
	block_size = block_size * 1024 * 1024

	// Create and return the data structure for the new server
	new_server := new (Server)
	new_server.basedir = basedir
	new_server.block_size = block_size
	return new_server
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
	if (ds == nil) {
		var rc (uint64) = 0
		return rc, err.ErrNotAvailable
	}
	return ds.block_size, err.NoErr
}
