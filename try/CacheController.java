import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

/**
 * CacheController Class, used to manipulate my cache
 */
public class CacheController {
    public Map<String, Node> versionMap;
    private Node head;
    private Node tail;
    private long remainSize;
    private long totalSize;
    private ServerInterface server;
    /**
     * Constructor
     *
     * @param size Long indicate the size of cache
     * @param server ServerInterface used to communicate with server
     */
    public CacheController(long size, ServerInterface server) {
        head = null;
        tail = null;
        versionMap = new HashMap<>();
        totalSize = size;
        remainSize = size;
        this.server = server;
    }

    /**
     * Used to deal with cache when open function called
     *
     * @param writePath String of writePath of file
     * @param readPath String of readPath of file
     * @param relativePath String of relativePath of file
     * @param readOnly boolean to indicate if the file is readOnly
     * @param version Integer to indicate the latest version of file
     * @param size Long to indicate the size of file
     * @return Return 1 if successful, or return error code
     */
    public synchronized int openCache(String writePath, String readPath, String relativePath, boolean readOnly, int version, long size) {
        File fileForRead = new File(readPath);
        /* check if the file is cached */
        if (!versionMap.containsKey(readPath)) {
            /* the file is too big to cache */
            if (size > totalSize) return FileHandling.Errors.ENOMEM;

            /* delete all useless old version */
            deleteOld(readPath);

            /* clean memory */
            if (size > remainSize) {
                /* clean over but still too small; */
                if (!cleanMem(size)) {
                    return FileHandling.Errors.ENOMEM;
                }
            }

            /* download it after checking */
            /* create father file folder if needed */
            if(fileForRead.getParentFile()!=null && !fileForRead.getParentFile().exists()) {
                new File(fileForRead.getParent()).mkdirs();
            }
            if (!createFile(fileForRead, relativePath, size)) return FileHandling.Errors.EINVAL;

            /* add to file controller */
            Proxy.LocalFileInfo localFileInfo = new Proxy.LocalFileInfo(relativePath, version, 0, size);
            Proxy.fileMap.put(readPath, localFileInfo);

            /* add to cache */
            Node node = new Node(size, readPath);
            versionMap.put(readPath, node);
            addNode(node);
        }

        /* for writer give them a special copy */
        if (!readOnly) {
            File fileForWrite = new File(writePath);
            try {
                /* clean mem */
                if (size > remainSize) {
                    if (!cleanMem(size)) {
                        return FileHandling.Errors.ENOMEM;
                    }
                }

                /* make the new copy */
                Files.copy(fileForRead.toPath(), fileForWrite.toPath());

                /* add to file controller */
                Proxy.LocalFileInfo localFileInfo = new Proxy.LocalFileInfo(relativePath, version, 0, size);
                Proxy.fileMap.put(writePath, localFileInfo);

                /* add to cache */
                Node node = new Node(size, writePath);
                versionMap.put(writePath,node);
                addNode(node);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 1;

    }

    /**
     * Used to delete old and no one using version of a file from cache
     *
     * @param readPath String of readPath of file
     */
    public void deleteOld(String readPath) {
        String fileName = readPath.split("-v")[0] + "-v";
        int latestVersion = Integer.parseInt(readPath.split("-v")[1]);
        for (int i = 1; i < latestVersion; i++) {
            String toDeletePath = fileName + i;
            File toDeleteFile = new File(toDeletePath);
            if (toDeleteFile.exists() && Proxy.fileMap.get(toDeletePath).numberOfUser <= 0) {
                Proxy.fileMap.remove(toDeletePath);
                toDeleteFile.delete();

                /* clean the cache */
                Node node = versionMap.get(toDeletePath);
                deleteNode(node);
                versionMap.remove(toDeletePath);
            }
        }
    }

    /**
     * Clean memory use LRU to release space
     *
     * @param size Long to indicate the size needed
     */
    public synchronized boolean cleanMem(long size) {
        if (remainSize >= size) return true;
        Node cur = head;
        Node tem;

        while (cur != null) {
            tem = cur.child;
            /* delete using LRU if no one use it */
            if (Proxy.fileMap.get(cur.readPath).numberOfUser <= 0)  {

                File file = new File(cur.readPath);
                file.delete();
                deleteNode(cur);
                versionMap.remove(cur.readPath);
            }
            if (remainSize >= size) return true;
            cur = tem;
        }
        return false;

    }

    /**
     * Download file from server
     *
     * @param file File object indicate the file needed to be downloaded
     * @param path String of relativePath of file needed to be downloaded
     * @param size Long to indicate the size needed
     */
    private boolean createFile(File file, String path, long size) {
        RandomAccessFile randomAccessFile;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        byte[] b;

        /* chunk the file and get from server */
        try {
            int chunkSize = 2000000;
            if (size > chunkSize) {
                long cont = 0;
                while (size >= chunkSize) {
                    b = server.getFile(path, chunkSize, cont);
                    cont += chunkSize;
                    randomAccessFile.write(b);
                    size -= chunkSize;
                }
                if (size > 0) {
                    b = server.getFile(path, (int) size, cont);
                    randomAccessFile.write(b);
                }
            } else {
                b = server.getFile(path, (int) size, 0);
                randomAccessFile.write(b);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Node used in LRU Cache, store the information of a file
     */
    public static class Node {
        boolean isOpen;
        String readPath;
        long size;
        Node father;
        Node child;

        /**
         * Constructor
         *
         * @param size Long to indicate the size of a file
         * @param readPath String of readPath of a file
         */
        public Node(long size, String readPath) {
            this.isOpen = false;
            this.readPath = readPath;
            this.size = size;
        }
    }

    /**
     * Add node to our LRU cache
     *
     * @param node Node that needed to be added
     */
    public void addNode(Node node) {
        if (node == null) return;
        remainSize -= node.size;
        if (head == null) {
            head = tail = node;
        } else {
            tail.child = node;
            node.father = tail;
            tail = node;
        }
    }

    /**
     * Delete node to our LRU cache
     *
     * @param node Node that needed to be deleted
     */
    public void deleteNode(Node node) {
        if (node == null) return;
        remainSize += node.size;
        if (head == tail) {
            head = null;
            tail = null;
        } else if (head == node) {
            head = node.child;
            node.child = null;
            head.father = null;
        } else if (tail == node) {
            tail = node.father;
            node.father = null;
            tail.child = null;
        } else {
            node.father.child = node.child;
            node.child.father = node.father;
            node.father = null;
            node.child = null;
        }
    }

    /**
     * Change the node's position after using
     *
     * @param fullPath String of file that is used
     */
    public void useNode(String fullPath) {
        Node node = versionMap.get(fullPath);
        deleteNode(node);
        addNode(node);
    }


}
