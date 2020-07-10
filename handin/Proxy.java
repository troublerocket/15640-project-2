import java.io.*;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Proxy Class
 */
class Proxy {
	static String cache_dir;
	private static int cache_size;
	static ServerInterface server;
	static myCache cache;
	static int global_fd;
	static final Object sync_object = new Object();

	// file descriptor and the file information map
	static Map<Integer, user_file> fd_map;
	// file path and the file cache information map
	static Map<String, local_file> file_map;

	private static class FileHandler implements FileHandling {

		/**
		 * Transfer the original path to an array of local cache paths
		 *
		 * @param path the original path
		 * @param version the latest version number 
		 * @param fd file descriptor
		 * @return An array of local read path, write path, and relative path
		 */
		public String[] path_transfer(String path, int version, int fd) {
			
			path = cache_dir + '/' + path;
			File file = new File(path);
			try {
				path = file.getCanonicalPath();
			} catch(IOException e){
				e.printStackTrace();
			}
			// path used for read file
			String read_path = path + "-v" + version;
			// path used for write file
			String write_path = read_path + "-u" + fd;
			// sub path in the cache
			String relative_path = 
				path.substring(cache_dir.length() + 1, path.length());

			return new String[]{read_path, write_path, relative_path};
		}

		/**
		 * Check the validation of a path, 
		 * whether it is in the right cache diretory
		 *
		 * @param path the full path
		 * @return True if it is valid, otherwise return false
		 */
		private boolean path_valid(String path) {
			return path.contains(cache_dir);
		}

		/**
		 * Get file information from the server when open function is called
		 *
		 * @param path the file path
		 * @return An array of file information
		 */
		private long[] get_info(String path) {
			try {
				// retrieve the the file from server
				return server.get_info(path);
			} catch (RemoteException e){
				e.printStackTrace();
				return null;
			}
		}

		/**
		 * Open function
		 *
		 * @param path the original path
		 * @param o open option
		 * @return The file descriptor
		 */
		public synchronized int open(String path, OpenOption o) {
			if (path == null) {
				return Errors.EINVAL;
			}
			// get file information for further processing
			long[] file_info = get_info(path);
			if (file_info == null) {
				return Errors.EINVAL;
			}

			user_file user_file;
			String option = "";
			int fd;
			RandomAccessFile randomAccessFile;
			// whether the file can be new
			boolean check_new = false;
			// whether the file must be new 
			boolean exclusive_new = false;
			// whether the file exists
			boolean file_exist = (file_info[0] == 1);
			// whether the file denoted by the path is a directory
			boolean file_dir = (file_info[1] == 1);
			// file size
			long size = file_info[2];
			// the latest version number
			int version = (int)file_info[3];

			switch (o) {
				case READ: // open for read access
					option = "r";
					break;
				case WRITE: // open for write access
					option = "rw";
					break;
				case CREATE: // create a new file if it does not exist
					option = "rw";
					check_new = true;
					break;
				case CREATE_NEW: // create a new file failing if already exists
					option = "rw";
					check_new = true;
					exclusive_new = true;
					break;
				default:
					return Errors.EINVAL;
			}
			// whether it is only for read access
			boolean read_only = option.equals("r");

			// synchronization for multi-threads to get the updated fd
			synchronized (sync_object) {
				global_fd++;
				fd = global_fd;
			}

			// transfer original path to paths for local/cache use
			String[] path_trans = path_transfer(path, version, fd);
			String read_path = path_trans[0];
			String write_path = path_trans[1];
			String full_path = "";
			if(read_only == true)
				full_path = read_path + "";
			else
				full_path = write_path + "";
			String relative_path = path_trans[2];

			File file = new File(full_path);

			// handle errors
			if (!path_valid(full_path)) 
				return Errors.EPERM;
			if (file_dir && !read_only) 
				return Errors.EISDIR;

			if (!file_dir) {
				if (file_exist) {
					if (exclusive_new) {
						return Errors.EEXIST;
					}
				} else {
					if (!check_new) {
						return Errors.ENOENT;
					}
					try {
						// create parent directory if it does not exist
						if(file.getParentFile()!=null 
							&& !file.getParentFile().exists()) {
							new File(file.getParent()).mkdirs();
						}
						// create the corresponding file on server
						server.create_file(relative_path);
					} catch (IOException e) {
						return Errors.EBUSY;
					}
				}
				//prepare cache for the file
				int open_cache = cache.open_cache(write_path ,
						read_path,relative_path, read_only, version, size);
				if (open_cache != 1) 
					return open_cache;

				try {
					randomAccessFile = new RandomAccessFile(full_path, option);
				} catch (FileNotFoundException e) {
					return Errors.ENOENT;
				}
				// record the file infomration
				user_file = new user_file(relative_path, full_path,
									randomAccessFile, false, !read_only);
			} else {
				// record the directory
				user_file = new user_file(relative_path, 
											full_path,null,true,false);
			}

			// file usage count
			if (read_only) {
				file_map.get(read_path).user_count++;
			} else {
				file_map.get(write_path).user_count++;
			}
			// store the fd and file info mapping
			fd_map.putIfAbsent(fd, user_file);
			return fd;
		}

		/**
		 * Close function
		 *
		 * @param fd file descriptor 
		 * @return 0 if succeeded, otherwise error number
		 */
		public synchronized int close(int fd) {
			if (!fd_map.containsKey(fd)) {
				return Errors.EBADF;
			}
			// get file information mapped from fd
			user_file user_file = fd_map.get(fd);

			if (user_file.dir_flag) {// if it is a directory
				fd_map.remove(fd);
				return 0;
			}

			if (user_file.write_flag) {// if it has been overwritten
				try {
					long size = user_file.randomAccessFile.length();
					int version;
					byte[] bytes;
					int chunk_size = (int)1e6;

					// write the file to the server
					if (size <= chunk_size) { // if the file size fits
						bytes = new byte[(int)size];
						user_file.randomAccessFile.seek(0);
						user_file.randomAccessFile.read(bytes);
						// get the version number for this update
						version = server.write_to_file(user_file.relative_path, 
								bytes, 0, true);
					} else { // if too large, divide the file into chunks
						long off = 0;
						bytes = new byte[chunk_size];
						while (off < size - chunk_size) {
							user_file.randomAccessFile.seek(off);
							user_file.randomAccessFile.read(bytes);
							server.write_to_file(user_file.relative_path, 
							bytes, off, false);
							off += chunk_size;
						}
						// last chunk to send
						bytes = new byte[(int)(size - off)];
						user_file.randomAccessFile.seek(off);
						user_file.randomAccessFile.read(bytes);
						// get the version number after all chunks done
						version = server.write_to_file(user_file.relative_path, 
								bytes, off, true);
					}
					// handle errors
					if (version == -1) {
						return Errors.EPERM;
					}

					// invalidate all old versions and delete from cache
					cache.cache_delete(
						cache.cache_map.get(user_file.full_path));
					cache.cache_map.remove(user_file.full_path);
					String new_read_path = 
					cache_dir + '/' +  user_file.relative_path + "-v" + version;
					cache.delete_version(new_read_path);

					File file = new File(user_file.full_path);
					// add the latest file to the cache
					if (cache.evict_cache(size)) {
						// record the file cache infomration
						local_file new_file = new local_file(
							user_file.relative_path, version, 0, size);
						file_map.putIfAbsent(new_read_path, new_file);
						myCache.Node node = 
							new myCache.Node(size, new_read_path);
						cache.cache_map.put(new_read_path, node);
						cache.cache_add(node);
						file.renameTo(new File(new_read_path));
					} else {// cache size not enough for the file
						if (file.exists()) 
							// delete the local file without caching it
							file.delete();
					}

				} catch (IOException e) {
					e.printStackTrace();
					return Errors.EBUSY;
				}
			} else { // if it is only read accessed
				if (cache.cache_map.containsKey(user_file.full_path)) 
					// update the file cache status
					cache.cache_update(user_file.full_path);
			}
			// file usage count
			file_map.get(user_file.full_path).user_count--;

			try {
				user_file.randomAccessFile.close();
				fd_map.remove(fd);
			} catch (IOException e) {
				e.printStackTrace();
				return Errors.EBUSY;
			}

			return 0;
		}

		/**
		 * Write function
		 *
		 * @param fd file descriptor 
		 * @param buf byte array of data to write
		 * @return Return byte number written if succeeded, 
		 * otherwise error number
		 */
		public synchronized long write(int fd, byte[] buf) {
			// handle errors
			if (!fd_map.containsKey(fd)) {
				return Errors.EBADF;
			}
			user_file user_file = fd_map.get(fd);
			if (user_file.dir_flag) {
				return Errors.EISDIR;
			}
			if (!user_file.write_flag) {
				return Errors.EBADF;
			}
			if (buf == null) {
				return Errors.EINVAL;
			}

			try {
				user_file.randomAccessFile.write(buf);
				return buf.length;
			} catch (IOException e) {
				if (e.getMessage().equals("Bad file descriptor")) {
					return Errors.EBADF;
				}
				return Errors.EBUSY;
			}

		}

		/**
		 * Read function
		 *
		 * @param fd file descriptor 
		 * @param buf byte array of data to read
		 * @return Return byte number read if succeeded, 
		 * otherwise error number
		 */
		public long read(int fd, byte[]buf) {
			// handle errors
			if (buf == null) {
				return Errors.EINVAL;
			}
			if (!fd_map.containsKey(fd)) {
				return Errors.EBADF;
			}
			user_file user_file = fd_map.get(fd);
			if (user_file.dir_flag) {
				return Errors.EISDIR;
			}

			try {
				long len = (long) user_file.randomAccessFile.read(buf);
				if (len == -1) 
					return 0;
				return len;
			} catch (IOException e) {
				return Errors.EBUSY;
			}
		}

		/**
		 * Lseek function
		 *
		 * @param fd file descriptor 
		 * @param pos lseek position offset
		 * @param o lseek option
		 * @return Return lseek position if succeeded, or error number
		 */
		public long lseek(int fd, long pos, LseekOption o) {
			// handle errors
			if (!fd_map.containsKey(fd)) {
				return Errors.EBADF;
			}
			user_file user_file = fd_map.get(fd);
			if (user_file.dir_flag) {
				return Errors.EISDIR;
			}
			// the actual starting position depending on the lseek option
			long option_pos = pos;
			RandomAccessFile randomAccessFile = user_file.randomAccessFile;

			if (o.equals(LseekOption.FROM_CURRENT)) {
				try {
					option_pos = randomAccessFile.getFilePointer() + pos;
				} catch (IOException e) {
					return Errors.EBUSY;
				}
			}else if (o.equals(LseekOption.FROM_END)) {
				try {
					option_pos = randomAccessFile.length() - pos;
				} catch (IOException e) {
					return Errors.EBUSY;
				}
			}
			try {
				randomAccessFile.seek(option_pos);
			} catch (IOException e) {
				return Errors.EBUSY;
			}

			return option_pos;
		}

		/**
		 * Unlink function
		 *
		 * @param path the file path
		 * @return Return 1 if succeeded, or error number
		 */
		public int unlink(String path) {
			String full_path = cache_dir + '/' + path;
			// handle errors
			if (path == null) {
				return Errors.EINVAL;
			}
			File file = new File(full_path);
			if (file.isDirectory()) {
				return Errors.EISDIR;
			}

			String read_path = "";
			try {
				long[] info = server.get_info(path);
				String[] path_trans = path_transfer(path, (int)info[3], 3);
				read_path = path_trans[0];
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			// delete the file from cache
			File delete_file = new File(read_path);
			if (delete_file.exists()) {
				if (!file_map.containsKey(read_path) 
					|| file_map.get(read_path).user_count <= 0) {
					delete_file.delete();
					myCache.Node node 
						= cache.cache_map.get(read_path);
					cache.cache_delete(node);
					cache.cache_map.remove(read_path);
				}
			}

			// delete the file from server
			boolean delete_flag = false;
			try {
				delete_flag = server.unlink_file(path);
			} catch (RemoteException e) {
				e.printStackTrace();
				return Errors.ENOENT;
			}
			if (delete_flag) {
				return 1;
			}
			else return Errors.ENOENT;
		}

		/**
		 * Clientdone function
		 */
		public void clientdone() {
			System.err.println("this is clientdone");
		}

	}


	private static class FileHandlingFactory implements FileHandlingMaking {
		public FileHandling newclient() {
			return new FileHandler();
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 4) 
			return;

		global_fd = 2;
		fd_map = new ConcurrentHashMap<>();
		file_map = new ConcurrentHashMap<>();
		File file = new File(args[2]);
		cache_dir = file.getCanonicalPath();
		try {
			cache_size = Integer.parseInt(args[3]);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}

		try {
			String addr = args[0];
			String port = args[1];
			server = (ServerInterface) Naming.lookup(
				"//" + addr+ ":" + port + "/server");
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

		cache = new myCache(cache_size, server);

		(new RPCreceiver(new FileHandlingFactory())).run();
	}



	/**
	 * user_file class for storing file information 
	 */
	private static class user_file {
		String relative_path;
		String full_path;
		RandomAccessFile randomAccessFile;
		boolean dir_flag;
		boolean write_flag;
		/**
		 * Constructor
		 *
		 * @param relative_path relativepath of file
		 * @param full_path full path of file
		 * @param randomAccessFile RandomAccessFile object of file
		 * @param dir_flag whether it is a directory
		 * @param write_flag whether it is writable
		 */
		user_file(String relative_path, String full_path, 
		RandomAccessFile randomAccessFile, boolean dir_flag, 
		boolean write_flag) {
			this.relative_path = relative_path;
			this.full_path = full_path;
			this.randomAccessFile = randomAccessFile;
			this.dir_flag = dir_flag;
			this.write_flag = write_flag;
		}
	}

	/**
	 * local_file Class for storing the file cache information
	 */
	static class local_file {
		String relative_path;
		long size;
		int version;
		int user_count;
		/**
		 * Constructor
		 *
		 * @param relative_path relative path of file
		 * @param version version number of file
		 * @param user_count number of current users
		 * @param size size of file
		 */
		local_file(String relative_path, int version, 
					int user_count, long size) {
			this.relative_path = relative_path;
			this.version = version;
			this.user_count = user_count;
			this.size = size;
		}
	}
}

