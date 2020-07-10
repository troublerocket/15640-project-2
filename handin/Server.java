import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Server class
 */
public class Server extends UnicastRemoteObject implements ServerInterface {
    // root path of server
    private static String root_path;
    // file path and synchronized object mapping
    private static Map<String, Object> sync_map;
    // file path and version number mapping
    private static Map<String, Integer> version_map;

    /**
     * Constructor
     */
    public Server() throws RemoteException{
    }

    public static void main(String[] args) throws RemoteException {
        if (args.length != 2) 
            return;
        File file = new File(args[1]);
        // initialization
        try {
            root_path = file.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        sync_map = new ConcurrentHashMap<>();
        version_map = new ConcurrentHashMap<>();
        int port = 0;
        try {
            port = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return;
        }

        try {
            LocateRegistry.createRegistry(port);
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        Server server = new Server();
        try {
            Naming.rebind("//127.0.0.1:" + port + "/server", server);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the file for the client
     *
     * @param path relative path of file on server
     * @param len length of file
     * @param off file position offset
     * @return An array of bytes of the file
     */
    @Override
    public byte[] get_file(String path, int len, long off)
        throws RemoteException {
        // the full path
        path = root_path + '/'+ path;
        File file = new File(path);
        try {
            // store the file path and its sync object mapping
            sync_map.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = new byte[len];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(off);
            randomAccessFile.read(bytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bytes;
    }

    /**
     * Get the size of file
     *
     * @param path relative path of file on server
     * @return size of the file
     */
    private long get_file_len(String path) throws RemoteException {
        File file = new File(path);
        return file.length();
    }

    /**
     * Create file on server
     *
     * @param path relative path of file on server
     */
    @Override
    public void create_file(String path) throws RemoteException {
        // the full path
        path = root_path + '/' + path;
        File file = new File(path);
        try {
            // store the file path and its sync object mapping
            sync_map.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e){
            e.printStackTrace();
        }
        if(file.getParentFile() != null && !file.getParentFile().exists()) {
            // create parent directory if it does not exist
            new File(file.getParent()).mkdirs();
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write file to server
     *
     * @param path relative path of file on server
     * @param buf byte number to write
     * @param off file position offset
     * @param flag whether the whole file write is done
     * @return The latest version number of file
     */
    @Override
    public int write_to_file(String path, byte[] buf, long off, boolean flag)
    throws RemoteException{
        // the full path
        path = root_path + '/' + path;
        File file = new File(path);
        try {
            // store the file path and its sync object mapping
            sync_map.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(file.getParentFile() != null && !file.getParentFile().exists()) {
            // create parent directory if it does not exist
            new File(file.getParent()).mkdirs();
        }

        RandomAccessFile randomAccessFile;
        // synchronization for multi-threads to write the file in order
        synchronized(sync_map.get(path)) {
            try {
                randomAccessFile = new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return -1;
            }
            try {
                randomAccessFile.seek(off);
                randomAccessFile.write(buf);
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
            // store the file path and version number mapping
            version_map.putIfAbsent(path, 0);
            if (flag) // update file version number when write is done
                version_map.put(path, version_map.get(path) + 1);
            return version_map.get(path);
        }
    }

    /**
     * Unlink a file from server
     *
     * @param path relative path of file on server
     * @return True if succeeded, otherwise return false
     */
    @Override
    public boolean unlink_file(String path) throws RemoteException {
        // the full path
        path = root_path + '/' + path;
        File file = new File(path);
        try {
            // store the file path and its sync object mapping
            sync_map.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }
        // synchronization for multi-threads to unlink the file in order
        synchronized (sync_map.get(path)) {
            if (version_map.containsKey(path))
                version_map.remove(path);
            return file.exists() && file.delete();
        }
    }

    /**
     * Get the latest version number of file
     *
     * @param path relative path of file on server
     * @return The latest version number of file
     */
    private int get_version(String path) throws RemoteException {
        version_map.putIfAbsent(path, 1);
        return version_map.get(path);
    }

    /**
     * Get information of file
     *
     * @param path relative path of file on server
     * @return An array of file information
     */
    @Override
    public long[] get_info(String path) throws RemoteException {

        path = root_path + '/' + path;
        File file = new File(path);
        try {
            path = file.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // whether the file exists and whether it is a directory
        long  file_exist, file_dir;
        // the file size
        long file_len =  get_file_len(path);
        // the latest version number of the file
        int file_ver = get_version(path);

        if(file.exists())
            file_exist = 1;
        else
            file_exist = 0;
        if(file.isDirectory())
            file_dir = 1;
        else
            file_dir = 0;
        return new long[]{file_exist, file_dir, file_len, (long)file_ver};
    }
}
