package it.uniroma2.adaptivescheduler.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

public class SimpleZookeeperClient {

    private static final int IGNORE_VERSION = -1;
    private static final int SESSION_TIMEOUT = 3000;
    private static final byte[] EMPTY_NODE = "".getBytes();

    private ZooKeeper zk;

    public SimpleZookeeperClient(List<String> servers, int port) throws IOException {

	String connectionString = "";
	int added = 0;

	for (String s : servers) {
	    if (added != 0) {
		connectionString += ",";
	    }

	    connectionString += s + ":" + port;
	    added++;
	}

	zk = new ZooKeeper(connectionString, SESSION_TIMEOUT, new SimpleWatcher());

    }
    public SimpleZookeeperClient(List<String> servers, int port,int timeout) throws IOException {

	String connectionString = "";
	int added = 0;

	for (String s : servers) {
	    if (added != 0) {
		connectionString += ",";
	    }

	    connectionString += s + ":" + port;
	    added++;
	}

	zk = new ZooKeeper(connectionString, timeout, new SimpleWatcher());

    }

    public boolean isConnected() {
	States s = zk.getState();

	return s.isConnected();
    }

    private String fullPath(String path, String child) {

	String childAbsPath = path;
	if (!path.endsWith("/")) {
	    childAbsPath += "/";
	}
	childAbsPath += child;

	return childAbsPath;

    }

    private String parentPath(String dirname) {

	if (dirname == null) {
	    return null;
	}

	String[] parts = dirname.split("/");
	String parent = "";

	if (parts.length < 3) {
	    /* dirname was /something, no parent */
	    return null;
	}

	/* everything but last */
	for (int i = 1; i < (parts.length - 1); i++) {
	    if (parts[i].equals(""))
		continue;

	    parent += "/" + parts[i];
	}

	return parent;

    }

    public void mkdirs(String dirname) throws KeeperException, InterruptedException {

	if (dirname == null)
	    return;

	if (!dirname.equals("/") && (zk.exists(dirname, false) == null)) {
	    mkdirs(parentPath(dirname));
	    setData(dirname, EMPTY_NODE);
	}
    }

    public boolean exists(String dirname) {

	boolean exists = false;

	if (dirname == null)
	    return exists;

	try {
	    exists = (zk.exists(dirname, false) != null);
	} catch (KeeperException e) {
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return exists;

    }

    public void deleteRecursive(String dirname) {

	try {
	    if (zk.exists(dirname, false) != null) {

		List<String> children = zk.getChildren(dirname, false);

		if (children != null && !children.isEmpty()) {
		    for (String child : children) {
			deleteRecursive(fullPath(dirname, child));
		    }
		}

		zk.delete(dirname, IGNORE_VERSION);

	    }
	} catch (KeeperException e) {
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
    }

    /**
     * Set data to a zookeeper path; if node does not exist it will be created.
     * 
     * The parentPath must exists.
     * 
     * @param path
     * @param data
     * @return
     */
    public Stat setData(String path, byte[] data) {
	try {

	    if (zk.exists(path, false) != null) {
		return zk.setData(path, data, IGNORE_VERSION);
	    } else {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		return new Stat();
	    }

	} catch (KeeperException e) {
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return null;
    }

    public Stat setDataNonPersistent(String path, byte[] data) {

	    try {
		if (zk.exists(path, false) != null) {
		zk.delete(path, IGNORE_VERSION);
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		return new Stat();
		} else {
		zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		return new Stat();
		}
	    } catch (KeeperException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }



	return null;
    }

    /**
     * Retrieve data from a zookeeper node
     * 
     * @param path
     * @return
     */
    public byte[] getData(String path) {

	Stat zkStat = new Stat();
	byte[] returnedValue = null;

	try {
	    returnedValue = zk.getData(path, false, zkStat);
	} catch (KeeperException e) {
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return returnedValue;
    }

    /**
     * Retrieve children of a zookeeper path
     * 
     * @param path
     * @return
     */
    public List<String> getChildren(String path) {

	List<String> returnedValue = null;
	try {
	    returnedValue = zk.getChildren(path, false);
	} catch (KeeperException e) {
	    e.printStackTrace();
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return returnedValue;
    }

    public ZooKeeper getZK() {
	return zk;
    }
}
