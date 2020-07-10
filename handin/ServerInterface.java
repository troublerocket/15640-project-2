import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * ServerInterface interface
 */
public interface ServerInterface extends Remote {
    
    byte[] get_file(String path, int len, long seek) throws RemoteException;
    void create_file(String path) throws RemoteException;
    int write_to_file(String path, byte[] buf, long seek, boolean over) 
    throws RemoteException;
    boolean unlink_file(String path) throws RemoteException;
    long[] get_info(String path) throws RemoteException;
    
}