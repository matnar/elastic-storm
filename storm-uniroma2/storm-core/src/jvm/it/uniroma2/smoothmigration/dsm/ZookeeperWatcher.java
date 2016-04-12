package it.uniroma2.smoothmigration.dsm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import backtype.storm.Config;
import it.uniroma2.adaptivescheduler.zk.SimpleWatcher;
import it.uniroma2.smoothmigration.spout.SmoothSpoutWatcher;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.Utils;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.network.TaskStream;
import it.uniroma2.taos.commons.network.TaskTopo;
import it.uniroma2.taos.commons.persistence.MongoManager;

public class ZookeeperWatcher {
    
    private String path = null;
    private byte[] data;
    private boolean createWait = false;
    private String waitPath = null;
    private byte[] waitData;
    private String rdyPath;
    private byte[] rdyData;

    // private static CuratorFramework cf = null;
    private static long refreshConnectionTime=0;
    private static long refreshConnectionTimeStart=0;
    private static String hostname = null;
    private static MongoManager manager = null;
    private static ZooKeeper zoo = null;
    private static Integer counter = 0;
    private static ArrayList<TaskTopo> readyTask = new ArrayList<TaskTopo>();
    private static ArrayList<TaskTopo> smoothTask = new ArrayList<TaskTopo>();
    private static ArrayList<TaskTopo> migratingTask = new ArrayList<TaskTopo>();
    private static ArrayList<TaskTopo> waitTask = new ArrayList<TaskTopo>();
    private static boolean enableLog = false;
    private static PlacementResponse pr = new PlacementResponse();
    private static ObjectMapper mapper = new ObjectMapper();
    private static String topoId;

    Thread pooler = null;

    public synchronized static long getLastRefreshTime()
    {
	return refreshConnectionTime;
    }
    public synchronized static void beginRefresh()
    {
	refreshConnectionTimeStart=System.currentTimeMillis();
    }
    public synchronized static void endRefresh()
    {
	refreshConnectionTime=refreshConnectionTimeStart;
    }
    
    public synchronized static void handleKeeperSessionExpired() {
	Logging.append("Resolving session Expire", Level.INFO, ZookeeperWatcher.class);
	try {
	    zoo.close();
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	connectZk();
	while (true) {
	    if (zoo.getState().isConnected()) {
		break;
	    }
	    try {
		Thread.sleep(100);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}

    }

    public ZookeeperWatcher(String topoId, boolean registerListener) {
	Logging.append("Starting Zookeeper", Level.INFO, ZookeeperWatcher.class);
	if (zoo == null) {
	    this.topoId = topoId;
	    String[] command = { "hostname" };
	    String result = Utils.executeCommand(command);
	    hostname = result.substring(0, result.length() - 1);
	    connectMongo();
	    connectZk();
	    getNow();
	    pooler = new Thread(new Runnable() {
		@Override
		public void run() {
		    while (true) {
			try {
			    Thread.sleep(1000);
			} catch (InterruptedException e) {
			    // TODO Auto-generated catch block
			    e.printStackTrace();
			}
			getNow();
		    }
		}
	    });
	    pooler.start();

	}
	if (registerListener) {
	    synchronized (counter) {
		counter++;
	    }

	}

    }

    public void shutdown() {
	synchronized (counter) {
	    counter--;
	}
	if (counter == 0) {
	    try {
		zoo.close();
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}

    }

    public void connectMongo() {
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	String mongoIp = (String) confs.get(Config.TAOS_MONGO_DB_IP);
	Integer mongoPort = (Integer) confs.get(Config.TAOS_MONGO_DB_PORT);
	int ttl = (Integer) confs.get(Config.TAOS_DB_TTL);
	Double updateDamping = (Double) confs.get(Config.TAOS_METRICS_SMOOTHING);
	manager = new MongoManager(mongoIp, mongoPort, ttl, updateDamping.floatValue());
    }

    public static void connectZk() {
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	Integer zkport = (Integer) confs.get(Config.STORM_ZOOKEEPER_PORT);
	Object obj = confs.get(Config.STORM_ZOOKEEPER_SERVERS);
	List<String> servers = (List<String>) obj;
	String connectionString = "";
	int added = 0;

	for (String s : servers) {
	    if (added != 0) {
		connectionString += ",";
	    }

	    connectionString += s + ":" + zkport;
	    added++;
	}
	try {
	    zoo = new ZooKeeper(connectionString, 30000, new SimpleWatcher());
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

    class CListener implements ConnectionStateListener {

	@Override
	public void stateChanged(CuratorFramework arg0, ConnectionState arg1) {

	    if (arg1.equals(ConnectionState.RECONNECTED)) {
		getNow();
		declareSmooth(null, null);
		createWait(null, null);
		deleteWait(null);
		createReady(null, null);
	    }

	}

    }

    public static void getNow() {
	synchronized (zoo) {
	    List<String> waitChilds = new ArrayList<String>();

	    try {
		waitChilds = zoo.getChildren("/sm/wait", false);
	    } catch (KeeperException e) {
		if (e.getClass().equals(SessionExpiredException.class)) {
		    handleKeeperSessionExpired();
		    try {
			waitChilds = zoo.getChildren("/sm/wait", false);
		    } catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    } catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	    waitTask = new ArrayList<TaskTopo>();
	    if (waitChilds != null) {
		for (String wait : waitChilds) {
		    TaskTopo tt = new TaskTopo(Integer.parseInt(wait.split("@")[0]), wait.split("@")[1]);
		    if (topoId == null || tt.getTopoId().equals(topoId))
			waitTask.add(tt);

		}
	    }
	    if (enableLog)
		Logging.append("waitTask: " + waitTask.toString(), Level.INFO, ZookeeperWatcher.class);
	    List<String> migChilds = new ArrayList<String>();

	    try {
		migChilds = zoo.getChildren("/sm/migrating", false);
	    } catch (KeeperException e) {
		if (e.getClass().equals(SessionExpiredException.class)) {
		    handleKeeperSessionExpired();
		    try {
			migChilds = zoo.getChildren("/sm/migrating", false);
		    } catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    } catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	    migratingTask = new ArrayList<TaskTopo>();
	    if (migChilds != null) {
		for (String task : migChilds) {
		    try {
			byte[] result = getData("/sm/migrating/" + task);
			if (result != null && result.length != 0)
			    migratingTask = mapper.readValue(result, TypeFactory.collectionType(ArrayList.class, TaskTopo.class));
			if (topoId != null) {
			    ArrayList<TaskTopo> filterMigratingTasks = new ArrayList<TaskTopo>();
			    for (TaskTopo tt : migratingTask) {
				if (tt.getTopoId().equals(topoId)) {
				    filterMigratingTasks.add(tt);
				}
			    }
			    migratingTask = filterMigratingTasks;
			}
		    } catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		}
	    }
	    if (enableLog)
		Logging.append("allmigratingtasks: " + migratingTask.toString(), Level.INFO, ZookeeperWatcher.class);
	    List<String> rdychilds = new ArrayList<String>();

	    try {
		rdychilds = zoo.getChildren("/sm/ready", false);
	    } catch (KeeperException e) {
		if (e.getClass().equals(SessionExpiredException.class)) {
		    handleKeeperSessionExpired();
		    try {
			rdychilds = zoo.getChildren("/sm/ready", false);
		    } catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    } catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	    readyTask = new ArrayList<TaskTopo>();
	    if (rdychilds != null) {
		for (String taskTopo : rdychilds) {
		    TaskTopo tt = null;
		    try {
			Stat zkStat = new Stat();
			byte[] result = null;
			try {
			    result = zoo.getData("/sm/ready/" + taskTopo, false, zkStat);
			} catch (KeeperException e) {
			    if (e.getClass().equals(SessionExpiredException.class)) {
				if (e.getClass().equals(SessionExpiredException.class)) {
				    handleKeeperSessionExpired();
				    try {
					result = zoo.getData("/sm/ready/" + taskTopo, false, zkStat);
				    } catch (KeeperException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				    } catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				    }
				}
			    }
			} catch (InterruptedException e) {
			}
			if (result != null && result.length != 0)
			    tt = mapper.readValue(result, TaskTopo.class);

		    } catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		    if (tt != null && (topoId==null || tt.getTopoId().equals(topoId)))
			readyTask.add(tt);
		}
	    }
	    if (enableLog)
		Logging.append("migrated is " + readyTask.toString(), Level.INFO, ZookeeperWatcher.class);
	    List<String> smootchilds = new ArrayList<String>();

	    try {
		smootchilds = zoo.getChildren("/sm/smooth", false);
	    } catch (KeeperException e) {
		if (e.getClass().equals(SessionExpiredException.class)) {
		    handleKeeperSessionExpired();
		    try {
			smootchilds = zoo.getChildren("/sm/smooth", false);
		    } catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    } catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	    smoothTask = new ArrayList<TaskTopo>();
	    if (smootchilds != null) {
		for (String taskTopo : smootchilds) {
		    TaskTopo tt = null;
		    try {
			byte[] result = getData("/sm/smooth/" + taskTopo);
			if (result != null && result.length != 0)
			    tt = mapper.readValue(result, TaskTopo.class);
		    } catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		    if (tt != null && (topoId==null || tt.getTopoId().equals(topoId)))
			smoothTask.add(tt);
		}
	    }
	    if (enableLog)
		Logging.append("smoothTask is " + smoothTask.toString(), Level.INFO, ZookeeperWatcher.class);
	    pr = manager.getDepl();

	}

    }

    public ArrayList<TaskTopo> getMigratingTask() {
	ArrayList<TaskTopo> toReturn = new ArrayList<TaskTopo>();
	toReturn.addAll(migratingTask);
	return toReturn;
    }

    public ArrayList<TaskTopo> getReadyTask() {

	ArrayList<TaskTopo> toReturn = new ArrayList<TaskTopo>();
	toReturn.addAll(readyTask);
	return toReturn;
    }

    public ArrayList<TaskTopo> getSmoothTask() {

	ArrayList<TaskTopo> toReturn = new ArrayList<TaskTopo>();
	toReturn.addAll(smoothTask);
	return toReturn;

    }

    public ArrayList<String> getMigStreams(String compId) {

	ArrayList<String> migStreams = new ArrayList<String>();
	if (pr != null && compId != null && topoId != null) {
	    migStreams = new ArrayList<String>();
	    ArrayList<TaskStream> outTaskStreams = pr.getOutputTaskStreams(topoId, compId);
	    for (TaskStream taskStream : outTaskStreams) {
		if (!migStreams.contains(taskStream.getStream())
			&& (waitTask.contains(new TaskTopo(taskStream.getTaskId(), topoId)) || migratingTask.contains(new TaskTopo(taskStream
				.getTaskId(), topoId)))) {
		    migStreams.add(taskStream.getStream());
		}
	    }
	    Logging.append("migStreams: " + migStreams.toString(), Level.INFO, SmoothSpoutWatcher.class);
	}
	return migStreams;
    }

    public ArrayList<TaskTopo> getWaitTask() {
	ArrayList<TaskTopo> toReturn = new ArrayList<TaskTopo>();
	toReturn.addAll(waitTask);
	return toReturn;

    }

    public PlacementResponse getPr() {
	return pr;

    }

    public void setEnableLog(boolean enableLog) {
	this.enableLog = enableLog;
    }

    /**
     * Retrieve data from a zookeeper node
     * 
     * @param path
     * @return
     */
    public static byte[] getData(String path) {

	Stat zkStat = new Stat();
	byte[] returnedValue = null;

	try {
	    returnedValue = zoo.getData(path, false, zkStat);
	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		try {
		    returnedValue = zoo.getData(path, false, zkStat);
		} catch (KeeperException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		} catch (InterruptedException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		}
	    }
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
	    returnedValue = zoo.getChildren(path, false);
	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		try {
		    returnedValue = zoo.getChildren(path, false);
		} catch (KeeperException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		} catch (InterruptedException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		}
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return returnedValue;
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

	    if (zoo.exists(path, false) != null) {
		return zoo.setData(path, data, -1);
	    } else {
		zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

		return new Stat();
	    }

	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		try {
		    if (zoo.exists(path, false) != null) {
			return zoo.setData(path, data, -1);
		    } else {
			zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

			return new Stat();
		    }
		} catch (KeeperException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		} catch (InterruptedException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		}
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return null;
    }

    public void mkdirs(String dirname) throws KeeperException, InterruptedException {

	if (dirname == null)
	    return;

	if (!dirname.equals("/") && (zoo.exists(dirname, false) == null)) {
	    mkdirs(parentPath(dirname));
	    setData(dirname, "".getBytes());
	}
    }

    public boolean exists(String dirname) {

	boolean exists = false;

	if (dirname == null)
	    return exists;

	try {
	    exists = (zoo.exists(dirname, false) != null);
	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		try {
		    exists = (zoo.exists(dirname, false) != null);
		} catch (KeeperException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		} catch (InterruptedException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		}
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}

	return exists;

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

    public void postMigratingTaskList(ArrayList<TaskTopo> MigratingTaskList) {
	try {
	    setData("/sm/migrating/migratingtasks", mapper.writeValueAsBytes(MigratingTaskList));
	} catch (JsonGenerationException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (JsonMappingException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void cleanUp() {
	try {
	    List<String> childs = getChildren("/sm/ready");
	    if (childs != null) {
		for (String c : childs) {
		    zoo.delete("/sm/ready/" + c, -1);
		}
	    }
	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		List<String> childs = getChildren("/sm/ready");
		if (childs != null) {
		    for (String c : childs) {
			try {
			    zoo.delete("/sm/ready/" + c, -1);
			} catch (InterruptedException e1) {
			    // TODO Auto-generated catch block
			    e1.printStackTrace();
			} catch (KeeperException e1) {
			    // TODO Auto-generated catch block
			    e1.printStackTrace();
			}
		    }
		}
	    }
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	try {
	    zoo.delete("/sm/migrating/migratingtasks", -1);
	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		try {
		    zoo.delete("/sm/migrating/migratingtasks", -1);
		} catch (InterruptedException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		} catch (KeeperException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		}
	    }
	}

    }

    public void createDirStructure() {
	try {
	    /* creazione delle cartelle alla base della smoothmigration */
	    mkdirs("/sm/migrating");
	    mkdirs("/sm/wait");
	    mkdirs("/sm/ready");
	    mkdirs("/sm/smooth");
	} catch (KeeperException e) {
	    if (e.getClass().equals(SessionExpiredException.class)) {
		handleKeeperSessionExpired();
		try {
		    mkdirs("/sm/migrating");
		    mkdirs("/sm/wait");
		    mkdirs("/sm/ready");
		    mkdirs("/sm/smooth");
		} catch (KeeperException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		} catch (InterruptedException e1) {
		    // TODO Auto-generated catch block
		    e1.printStackTrace();
		}
	    }

	} catch (InterruptedException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    public void declareSmooth(String path, byte[] data) {
	if (path != null) {
	    this.path = path;
	    this.data = data;
	}
	if (this.path != null)
	    setData(this.path, this.data);
    }

    public void deleteWait(String path) {
	if (path != null) {
	    this.waitPath = path;
	}
	if (this.waitPath != null && !createWait) {
	    try {
		zoo.delete(this.waitPath, -1);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (KeeperException e) {
		if (e.getClass().equals(SessionExpiredException.class)) {
		    handleKeeperSessionExpired();
		    try {
			zoo.delete(this.waitPath, -1);
		    } catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    } catch (KeeperException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}
	    }
	}

    }

    public void createWait(String path, byte[] data) {
	if (path != null) {
	    this.waitPath = path;
	    this.waitData = data;
	}
	if (this.waitPath != null) {
	    createWait = true;
	    setData(this.waitPath, this.waitData);
	}

    }

    public void createReady(String path, byte[] data) {
	if (path != null) {
	    this.rdyPath = path;
	    this.rdyData = data;
	}
	if (this.rdyPath != null) {
	    setData(this.rdyPath, this.rdyData);
	}
    }

    public ZooKeeper getZoo() {
	return zoo;
    }

    public MongoManager getManager() {
	return manager;
    }

    public static String getHostname() {
	return hostname;
    }

    public static void setHostname(String hostname) {
	ZookeeperWatcher.hostname = hostname;
    }
    public static String getTopoId() {
        return topoId;
    }


}
