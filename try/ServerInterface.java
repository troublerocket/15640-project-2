import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * ServerInterface
 */
public interface ServerInterface extends Remote {
    byte[] getFile(String path, int len, long seek) throws RemoteException;
    void createFile(String path) throws RemoteException;
    int writeBack(String path, byte[] buf, long seek, boolean over) throws RemoteException;
    boolean unlink(String path) throws RemoteException;
    long[] getOpenInfo(String path) throws RemoteException;
}