import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;

/**
 * myCache Class
 */
public class myCache {
    // file path and cache node mapping
    public Map<String, Node> cache_map;
    // cache storage head
    private Node head;
    // cache storage tail
    private Node tail;
    // remaining size of cache
    private long size_remain;
    // total size of cache
    private long size_total;
    // the server interface
    private ServerInterface server;
    /**
     * Constructor
     *
     * @param size the size of cache
     * @param server interface for communication with server
     */
    public myCache(long size, ServerInterface server) {
        head = null;
        tail = null;
        cache_map = new HashMap<>();
        size_total = size;
        size_remain = size;
        this.server = server;
    }

    /**
     * Prepare the cache for open function 
     *
     * @param write_path write path of file
     * @param read_path read path  of file
     * @param relative_path relative path of file
     * @param read_only whether the file is only for read access
     * @param version the latest version number of file
     * @param size the size of file
     * @return Return 1 if succeeded, otherwise return error number
     */
    public synchronized int open_cache(String write_path, String read_path, 
            String relative_path, boolean read_only, int version, long size) {
        File read_file = new File(read_path);

        if (!cache_map.containsKey(read_path)) {// the file is not cached yet

            if (size > size_total)// file size is too large to cache
                return FileHandling.Errors.ENOMEM;
            // invalidate and delete all previous versions
            delete_version(read_path);

            if (size > size_remain) {// evict to cache the file
                if (!evict_cache(size)) {
                    return FileHandling.Errors.ENOMEM;
                }
            }

            if(read_file.getParentFile()!=null 
                && !read_file.getParentFile().exists()) {
                // create parent directory if it does not exist
                new File(read_file.getParent()).mkdirs();
            }
            if (!fetch_file(read_file, relative_path, size)) 
                return FileHandling.Errors.EINVAL;

            Proxy.local_file local_file = 
                        new Proxy.local_file(relative_path, version, 0, size);
            // store the file path and file cache info mapping
            Proxy.file_map.put(read_path, local_file);

            Node node = new Node(size, read_path);
            // store the file path and cache mapping
            cache_map.put(read_path, node);
            // add the file to cache storage
            cache_add(node);
        }

        if (!read_only) {// file is for write access
            File write_file = new File(write_path);
            try {
                if (size > size_remain) {
                    if (!evict_cache(size)) {
                        return FileHandling.Errors.ENOMEM;
                    }
                }
                // make a new copy
                Files.copy(read_file.toPath(), write_file.toPath());

                Proxy.local_file local_file = 
                        new Proxy.local_file(relative_path, version, 0, size);
                // store the file path and local file info mapping
                Proxy.file_map.put(write_path, local_file);

                Node node = new Node(size, write_path);
                // store the file path and cache mapping
                cache_map.put(write_path,node);
                // add to cache storage
                cache_add(node);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return 1;

    }

    /**
     * Invalidate and delete previous versions of file
     *
     * @param read_path read path of file
     */
    public void delete_version(String read_path) {
        String file_path = read_path.split("-v")[0] + "-v";
        // the latest version number
        int curr_version = Integer.parseInt(read_path.split("-v")[1]);
        for (int i = 1; i < curr_version; i++) {
            String delete_path = file_path + i;
            File delete_file = new File(delete_path);
            if (delete_file.exists() 
                && Proxy.file_map.get(delete_path).user_count <= 0) {
                Proxy.file_map.remove(delete_path);
                delete_file.delete();
                // delete from cache storage
                Node node = cache_map.get(delete_path);
                cache_delete(node);
                cache_map.remove(delete_path);
            }
        }
    }

    /**
     * evict files in cache based on LRU
     *
     * @param size size required to replace
     */
    public synchronized boolean evict_cache(long size) {
        if (size_remain >= size) 
            return true;
        Node curr = head;
        Node next;
        while (curr != null) {// evict from head(least recent)of cache storage
            next = curr.child;
            // check no user is visiting the file now
            if (Proxy.file_map.get(curr.read_path).user_count <= 0){
                File file = new File(curr.read_path);
                file.delete();
                cache_delete(curr);
                cache_map.remove(curr.read_path);
            }
            if (size_remain >= size) 
                return true;
            curr = next;
        }
        return false;

    }

    /**
     * Fetch file from server
     *
     * @param file file needed to be fetched
     * @param path relative path of file on server
     * @param size the size of file
     */
    private boolean fetch_file(File file, String path, long size) {
        RandomAccessFile randomAccessFile;
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        }

        byte[] bytes;

        try {
            int chunk_size = (int)2e6;
            if (size > chunk_size) { // if too large, fetch in chunks
                long pos = 0;
                while (size >= chunk_size) {
                    bytes = server.get_file(path, chunk_size, pos);
                    pos += chunk_size;
                    randomAccessFile.write(bytes);
                    size -= chunk_size;
                }
                if (size > 0) {
                    bytes = server.get_file(path, (int)size, pos);
                    randomAccessFile.write(bytes);
                }
            } else {// fetch the whole file
                bytes = server.get_file(path, (int)size, 0);
                randomAccessFile.write(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Node class
     */
    public static class Node {
        // read path of the file
        String read_path;
        // size of the fil
        long size;
        // the previous node
        Node parent;
        // the next node
        Node child;

        /**
         * Constructor
         *
         * @param size size of file
         * @param read_path read path of file
         */
        public Node(long size, String read_path) {
            this.read_path = read_path;
            this.size = size;
        }
    }

    /**
     * Add cache node to cache
     *
     * @param node cache node to add
     */
    public void cache_add(Node node) {
        if (node == null) 
            return;
        size_remain -= node.size;
        if (head == null) {// cold cache
            head = tail = node;
        } else {// add the node to tail
            tail.child = node;
            node.parent = tail;
            tail = node;
        }
    }

    /**
     * Delete cache node from cache
     *
     * @param node cache node to delete
     */
    public void cache_delete(Node node) {
        if (node == null)
            return;
        size_remain += node.size;
        if (head == tail) {// cold cache
            head = null;
            tail = null;
        } else if (head == node) { // the first node
            head = node.child;
            node.child = null;
            head.parent = null;
        } else if (tail == node) { // the last node
            tail = node.parent;
            node.parent = null;
            tail.child = null;
        } else {
            node.parent.child = node.child;
            node.child.parent = node.parent;
            node.parent = null;
            node.child = null;
        }
    }

    /**
     * update cache node after use based on LRU
     *
     * @param path the path of file
     */
    public void cache_update(String path) {
        Node node = cache_map.get(path);
        // remove it and add it again to the tail
        cache_delete(node);
        cache_add(node);
    }


}
