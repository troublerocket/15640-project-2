import java.rmi.Remote;
import java.io.*;
import java.rmi.RemoteException;

public interface IServer extends Remote {
	public String sayHello() throws RemoteException;
	public int getVersion(String path) throws RemoteException;
	public byte[] downloadFile(String path, long n, int len) throws RemoteException;
	public boolean uploadFile(String path, byte[] buffer, long pos, int len) throws RemoteException;
	public int rmFile(String path) throws RemoteException;
	public int getFileLen(String path) throws RemoteException;
	public String getRootPath() throws RemoteException, IOException;
}
