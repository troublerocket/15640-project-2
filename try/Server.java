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
 * Server
 */
public class Server extends UnicastRemoteObject implements ServerInterface {
    private static String fileroot;
    private static Map<String, Object> lockerMap;
    private static Map<String, Integer> versionMap;

    /**
     * Constructor
     */
    public Server() throws RemoteException {
    }

    public static void main(String[] args) throws RemoteException {
        if (args.length != 2) return;
        File file = new File(args[1]);
        try {
            fileroot = file.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        lockerMap = new ConcurrentHashMap<>();
        versionMap = new ConcurrentHashMap<>();
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
     * Deliver whole file to client
     *
     * @param path String of relativePath of file
     * @param len Integer to indicate size to deliver
     * @param seek Long to indicate the position from where to deliver
     * @return Arrays of bytes to be delivered
     */
    @Override
    public byte[] getFile(String path, int len, long seek) throws RemoteException {
        path = fileroot + '/'+ path;
        File file = new File(path);
        try {
            lockerMap.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] out = new byte[len];
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(seek);
            randomAccessFile.read(out);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out;
    }

    /**
     * Get the size of a file
     *
     * @param path String of relativePath of a file
     * @return Long integer indicate the size of a file
     */
    private long getLength(String path) throws RemoteException {
        File file = new File(path);
        return file.length();
    }

    /**
     * Create file on server
     *
     * @param path String of relativePath of a file
     */
    @Override
    public void createFile(String path) throws RemoteException {
        path = fileroot + '/' + path;
        File file = new File(path);
        try {
            lockerMap.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(file.getParentFile()!=null && !file.getParentFile().exists()) {
            new File(file.getParent()).mkdirs();
        }
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Write to file on server
     *
     * @param path String of relativePath of a file
     * @param buf Arrays of bytes to be written
     * @param seek Long to indicate the position from where to write
     * @param over Boolean indicate if deliver is over
     * @return Return Integer of latest version of this file
     */
    @Override
    public int writeBack(String path, byte[] buf, long seek, boolean over) throws RemoteException{
        path = fileroot + '/' + path;
        File file = new File(path);

        try {
            lockerMap.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(file.getParentFile()!=null && !file.getParentFile().exists()) {
            new File(file.getParent()).mkdirs();
        }

        RandomAccessFile randomAccessFile;
        synchronized (lockerMap.get(path)) {
            try {
                randomAccessFile = new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return -1;
            }

            try {
                randomAccessFile.seek(seek);
                randomAccessFile.write(buf);
            } catch (IOException e) {
                e.printStackTrace();
                return -1;
            }
            /* update the version if the write process is over */
            versionMap.putIfAbsent(path, 0);
            if (over) versionMap.put(path, versionMap.get(path) + 1);
            return versionMap.get(path);
        }
    }

    /**
     * Unlink a file from server
     *
     * @param path String of relativePath of a file
     * @return Return true if successful, or return false
     */
    @Override
    public boolean unlink(String path) throws RemoteException {
        path = fileroot + '/' + path;
        File file = new File(path);
        try {
            lockerMap.putIfAbsent(file.getCanonicalPath(), new Object());
        } catch (IOException e) {
            e.printStackTrace();
        }
        synchronized (lockerMap.get(path)) {
            if (versionMap.containsKey(path)) versionMap.remove(path);
            return file.exists() && file.delete();
        }
    }

    /**
     * Get latest version number of a file
     *
     * @param path String of relativePath of file
     * @return Return the latest version number
     */
    private int version(String path) throws RemoteException {
        versionMap.putIfAbsent(path, 1);
        return versionMap.get(path);
    }

    /**
     * Get some information of file
     *
     * @param path String of relativePath of file
     * @return Arrays of long Integer indicate if a file exist, is directory, length of a file, and the latest version
     */
    @Override
    public long[] getOpenInfo(String path) throws RemoteException {

        path = fileroot + '/' + path;
        File file = new File(path);
        try {
            path = file.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new long[]{file.exists() ? 1 : 0, file.isDirectory() ? 1 : 0, getLength(path), (long)version(path)};
    }
}
