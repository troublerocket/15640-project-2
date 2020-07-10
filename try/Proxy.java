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
	static String cacheDir;
	private static int caheSize;
	static ServerInterface server;
	static CacheController cache;
	static int globalfd;
	static final Object locker = new Object();

	/* map to store fd -> UserFileInfo */
	static Map<Integer, UserFileInfo> fdMap;
	
	/* map to store readPath -> LocalFileInfo */
	static Map<String, LocalFileInfo> fileMap;

	private static class FileHandler implements FileHandling {

		/**
		 * Transfer original input path to readPath, writePath and clean relativePath
		 *
		 * @param path String of original input path
		 * @param latestVersion latest version number Integer
		 * @param fd file descriptor Integer
		 * @return String arrays of readPath, writePath, and relativePath
		 */
		public String[] pathFatory(String path, int latestVersion, int fd) {
			path = cacheDir + '/' + path;

			File file = new File(path);
			try {
				path = file.getCanonicalPath();
			} catch (IOException e) {
				e.printStackTrace();
			}

			String pathRead = path + "-v" + latestVersion;
			String pathWrite = pathRead + "-u" + fd;
			String relativePath = path.substring(cacheDir.length() + 1, path.length());

			return new String[]{pathRead, pathWrite, relativePath};
		}

		/**
		 * Check whether the path is under the permitted folder
		 *
		 * @param path String of original input path
		 * @return Return true if it is valid, or return false
		 */
		private boolean checkFilePath(String path) {
			return path.contains(cacheDir);
		}

		/**
		 * Get open information when open
		 *
		 * @param path String of original input path
		 * @return Return arrays of long indicate file existence, is file a dir, file length, and latest file version
		 */
		private long[] getOpenInfo(String path) {
			try {
				return server.getOpenInfo(path);
			} catch (RemoteException e) {
				e.printStackTrace();
				return null;
			}
		}

		/**
		 * Open function
		 *
		 * @param path String of original input path
		 * @param o OpenOption input when open
		 * @return Return file descriptor
		 */
		public synchronized int open( String path, OpenOption o ) {
			System.err.println("this is open : " + path +" "+ o);
			if (path == null) {
				return Errors.EINVAL;
			}

			/* get open information at beginning */
			long[] openInfo = getOpenInfo(path);
			if (openInfo == null) {
				return Errors.EINVAL;
			}

			/* init local variables */
			UserFileInfo userFileInfo;
			String flag = "";
			int fd;
			RandomAccessFile randomAccessFile;
			boolean couldNew = false;
			boolean mustNew = false;
			boolean fileExist = openInfo[0] == 1;
			boolean fileIsDir = openInfo[1] == 1;
			long size = openInfo[2];
			int version = (int)openInfo[3];

			/* set open option */
			switch (o) {
				case READ:
					flag = "r";
					break;
				case WRITE:
					flag = "rw";
					break;
				case CREATE:
					flag = "rw";
					couldNew = true;
					break;
				case CREATE_NEW:
					flag = "rw";
					couldNew = true;
					mustNew = true;
					break;
				default:
					return Errors.EINVAL;
			}

			boolean readOnly = flag.equals("r");

			/* get update global fd and get it */
			synchronized (locker) {
				globalfd++;
				fd = globalfd;
			}

			/* get designed full path name on proxy */
			String[] fullPaths = pathFatory(path, version, fd);
			String readPath = fullPaths[0];
			String writePath = fullPaths[1];
			String fullPath = readOnly ? readPath : writePath;
			String relativePath = fullPaths[2];
			File file = new File(fullPath);

			/* check whether the path is in the right scope */
			if (!checkFilePath(fullPath)) return Errors.EPERM;

			/* check file existence and is directory and open option */
			if (fileIsDir && !readOnly) {
				return Errors.EISDIR;
			}
			if (!fileIsDir) {
				if (fileExist) {
					if (mustNew) {
						return Errors.EEXIST;
					}
				} else {
					if (!couldNew) {
						return Errors.ENOENT;
					}
					try {
						/* create father file folder if needed */
						if(file.getParentFile()!=null && !file.getParentFile().exists()) {
							new File(file.getParent()).mkdirs();
						}
						server.createFile(relativePath);
					} catch (IOException e) {
						return Errors.EBUSY;
					}
				}

				/* all checks are passed, and set cache */
				int openCache = cache.openCache(writePath ,readPath,relativePath, readOnly, version, size);
				if (openCache != 1) return openCache;

				try {
					randomAccessFile = new RandomAccessFile(fullPath, flag);
				} catch (FileNotFoundException e) {
					System.err.println("open File no file");
					return Errors.ENOENT;
				}
				/* the opened file is not a directory */
				userFileInfo = new UserFileInfo(relativePath, fullPath, randomAccessFile, false, !readOnly);
			} else {
				/* the opened file is a directory */
				userFileInfo = new UserFileInfo(relativePath, fullPath,null,true,false);
			}

			/* increase the number of file usage */
			if (readOnly) {
				fileMap.get(readPath).numberOfUser++;
			} else {
				fileMap.get(writePath).numberOfUser++;
			}
			fdMap.putIfAbsent(fd, userFileInfo);
			return fd;
		}

		/**
		 * Close function
		 *
		 * @param fd file descriptor Integer
		 * @return Return 0 if successful, or error code
		 */
		public synchronized int close( int fd ) {
			System.err.println("this is close : " + fd);

			if (!fdMap.containsKey(fd)) {
				return Errors.EBADF;
			}
			UserFileInfo userFileInfo = fdMap.get(fd);

			/* if the fd indicate a directory */
			if (userFileInfo.isDir) {
				fdMap.remove(fd);
				return 0;
			}

			/* for write file, which means version will change */
			if (userFileInfo.writable) {
				try {
					long size = userFileInfo.randomAccessFile.length();
					int latestVersion;
					byte[] b;

					int chunkSize = 1000000;

					/* chunk the file to be deliver to server */
					if (size <= chunkSize) {
						b = new byte[(int) size];
						userFileInfo.randomAccessFile.seek(0);
						userFileInfo.randomAccessFile.read(b);
						latestVersion = server.writeBack(userFileInfo.relativePath, b, 0, true);
					} else {
						long count = 0;
						b = new byte[chunkSize];
						while (count < size - chunkSize) {
							userFileInfo.randomAccessFile.seek(count);
							userFileInfo.randomAccessFile.read(b);
							server.writeBack(userFileInfo.relativePath, b, count, false);
							count += chunkSize;
						}
						/* get the latest version at the last deliver */
						b = new byte[(int)(size - count)];
						userFileInfo.randomAccessFile.seek(count);
						userFileInfo.randomAccessFile.read(b);
						latestVersion = server.writeBack(userFileInfo.relativePath, b, count, true);
					}
					/* check is write back successful */
					if (latestVersion == -1) {
						return Errors.EPERM;
					}

					/* delete from cache */
					CacheController.Node nodeToDelete = cache.versionMap.get(userFileInfo.fullPath);
					cache.deleteNode(nodeToDelete);
					cache.versionMap.remove(userFileInfo.fullPath);

					/* new name */
					String newReadPath = cacheDir + '/' +  userFileInfo.relativePath + "-v" + latestVersion;

					/* delete all useless old version */
					cache.deleteOld(newReadPath);

					File fileOri = new File(userFileInfo.fullPath);

					/* check cache is enough or not, if so rename write to new read file, else delete write file */
					if (cache.cleanMem(size)) {
						LocalFileInfo newLocalFileInfo = new LocalFileInfo(userFileInfo.relativePath, latestVersion, 0, size);
						fileMap.putIfAbsent(newReadPath, newLocalFileInfo);
						/* add to cache */
						CacheController.Node node = new CacheController.Node(size, newReadPath);
						cache.versionMap.put(newReadPath, node);
						cache.addNode(node);

						fileOri.renameTo(new File(newReadPath));
					} else {
						if (fileOri.exists()) fileOri.delete();
					}

				} catch (IOException e) {
					e.printStackTrace();
					return Errors.EBUSY;
				}
			} else {
				/* file is read only, just status of LRU cache */
				if (cache.versionMap.containsKey(userFileInfo.fullPath)) cache.useNode(userFileInfo.fullPath);
			}
			/* decrease file usage */
			fileMap.get(userFileInfo.fullPath).numberOfUser--;

			/* close everything else */
			try {
				userFileInfo.randomAccessFile.close();
				fdMap.remove(fd);
			} catch (IOException e) {
				e.printStackTrace();
				return Errors.EBUSY;
			}

			return 0;
		}

		/**
		 * Write function
		 *
		 * @param fd file descriptor Integer
		 * @param buf byte arrays of data to write
		 * @return Return array length written if successful, or error code
		 */
		public synchronized long write( int fd, byte[] buf ) {
			System.err.println("this is write : " + fd);
			if (!fdMap.containsKey(fd)) {
				System.err.println("write no such file descriptor");
				return Errors.EBADF;
			}
			UserFileInfo userFileInfo = fdMap.get(fd);

			/* check if the file is directory */
			if (userFileInfo.isDir) {
				return Errors.EISDIR;
			}

			/* check if the file is writable */
			if (!userFileInfo.writable) {
				return Errors.EBADF;
			}

			if (buf == null) {
				return Errors.EINVAL;
			}

			/* write */
			try {
				userFileInfo.randomAccessFile.write(buf);
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
		 * @param fd file descriptor Integer
		 * @param buf byte array to read
		 * @return Return byte number read if successful, or error code
		 */
		public long read( int fd, byte[] buf ) {
			System.err.println("this is read : " + fd);
			if (buf == null) {
				return Errors.EINVAL;
			}
			if (!fdMap.containsKey(fd)) {
				System.err.println("read no such file descriptor");
				return Errors.EBADF;
			}
			UserFileInfo userFileInfo = fdMap.get(fd);

			/* check if the file is directory */
			if (userFileInfo.isDir) {
				return Errors.EISDIR;
			}

			/* read */
			try {
				long length = (long) userFileInfo.randomAccessFile.read(buf);
				if (length == -1) return 0;
				return length;
			} catch (IOException e) {
				System.err.println("read IO error");
				return Errors.EBUSY;
			}
		}

		/**
		 * Lseek function
		 *
		 * @param fd file descriptor Integer
		 * @param pos Long data indicate position to lseek
		 * @param o LseekOption to determine lseek method
		 * @return Return latest position if successful, or error code
		 */
		public long lseek( int fd, long pos, LseekOption o ) {
			System.err.println("this is lseek : " + fd + " and  pos is : " + pos);
			if (!fdMap.containsKey(fd)) {
				System.err.println("lseek no such file descriptor");
				return Errors.EBADF;
			}

			UserFileInfo userFileInfo = fdMap.get(fd);

			/* check if the file is directory */
			if (userFileInfo.isDir) {
				return Errors.EISDIR;
			}

			long realPos = pos;
			RandomAccessFile randomAccessFile = userFileInfo.randomAccessFile;

			/* set real position of a file, it depends on lseek option */
			if (o.equals(LseekOption.FROM_CURRENT)) {
				try {
					realPos = randomAccessFile.getFilePointer() + pos;
				} catch (IOException e) {
					return Errors.EBUSY;
				}
			} else if (o.equals(LseekOption.FROM_END)) {
				try {
					realPos = randomAccessFile.length() - pos;
				} catch (IOException e) {
					return Errors.EBUSY;
				}
			}

			/* lseek */
			try {
				randomAccessFile.seek(realPos);
			} catch (IOException e) {
				return Errors.EBUSY;
			}

			return realPos;
		}

		/**
		 * Unlink function
		 *
		 * @param path String of original input path
		 * @return Return 1 if successful, or error code
		 */
		public int unlink( String path ) {
			System.err.println("this is unline : " + path);
			String fullPath = cacheDir + '/' + path;
			if (path == null) {
				return Errors.EINVAL;
			}

			File file = new File(fullPath);

			/* check if the file is directory */
			if (file.isDirectory()) {
				return Errors.EISDIR;
			}

			/* delete the cache */
			String readPath = "";
			try {
				long[] info = server.getOpenInfo(path);
				String[] paths = pathFatory(path, (int)info[3], 3);
				readPath = paths[0];
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			File toDelete = new File(readPath);
			if (toDelete.exists()) {
				if (!fileMap.containsKey(readPath) || fileMap.get(readPath).numberOfUser <= 0) {
					toDelete.delete();
					CacheController.Node node = cache.versionMap.get(readPath);
					cache.deleteNode(node);
					cache.versionMap.remove(readPath);
				}
			}


			boolean deleted = false;
			/* delete it on server */
			try {
				deleted = server.unlink(path);
			} catch (RemoteException e) {
				e.printStackTrace();
				return Errors.ENOENT;
			}

			if (deleted) {
				return 1;
			}
			else return Errors.ENOENT;
		}

		/**
		 * Client done function, indicate session broken
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
		// check arg numbers
		if (args.length != 4) return;

		// initiate all global fields
		globalfd = 2;
		fdMap = new ConcurrentHashMap<>();
		fileMap = new ConcurrentHashMap<>();

		File file = new File(args[2]);
		cacheDir = file.getCanonicalPath();

		try {
			caheSize = Integer.parseInt(args[3]);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		System.err.println("cache Size is : " + caheSize);

		// connect to Server
		try {
			server = (ServerInterface) Naming.lookup("//" + args[0] + ":" + args[1] + "/server");
		} catch (NotBoundException e) {
			e.printStackTrace();
		}

		// new Cache Controller
		cache = new CacheController(caheSize, server);

		// run
		(new RPCreceiver(new FileHandlingFactory())).run();
	}



	/**
	 * UserFileInfo class, used to store file information of each corresponding file descriptor
	 */
	private static class UserFileInfo {
		String relativePath;
		String fullPath;
		RandomAccessFile randomAccessFile;
		boolean isDir;
		boolean writable;
		/**
		 * Constructor
		 *
		 * @param relativePath String of relativePath of file
		 * @param fullPath String of fullPath of file
		 * @param randomAccessFile RandomAccessFile object of that file
		 * @param isDir boolean to indicate if the file is a directory
		 * @param writable boolean to indicate if this file writable
		 */
		UserFileInfo(String relativePath, String fullPath, RandomAccessFile randomAccessFile, boolean isDir, boolean writable) {
			this.relativePath = relativePath;
			this.fullPath = fullPath;
			this.randomAccessFile = randomAccessFile;
			this.isDir = isDir;
			this.writable = writable;
		}
	}

	/**
	 * LocalFileInfo Class, used to store the information of each file in my cache
	 */
	static class LocalFileInfo {
		String relativePath;
		long size;
		int version;
		int numberOfUser;
		/**
		 * Constructor
		 *
		 * @param relativePath String of relativePath of file
		 * @param version Integer to indicate the version of this file
		 * @param numberOfUser Integer to indicate the number of user that is using this file
		 * @param size Long to indicate the size of the file
		 */
		LocalFileInfo(String relativePath, int version, int numberOfUser, long size) {
			this.relativePath = relativePath;
			this.version = version;
			this.numberOfUser = numberOfUser;
			this.size = size;
		}
	}
}

