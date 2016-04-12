package it.uniroma2.taos.monitor;

import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import it.uniroma2.smoothmigration.dsm.ZookeeperWatcher;
import it.uniroma2.taos.commons.Constants;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.Utils;
import it.uniroma2.taos.commons.dao.ExecutorTuple;
import it.uniroma2.taos.commons.network.ExecutorTopo;
import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.persistence.MongoManager;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import backtype.storm.Config;
import backtype.storm.tuple.Tuple;

import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;
import com.higherfrequencytrading.chronicle.tools.ChronicleTools;

public class MonitorClient {
    private boolean clojureReporter = true;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private long lastMsg = 0;
    private IndexedChronicle chronicle = null;
    private ThreadMXBean thMxB = ManagementFactory.getThreadMXBean();
    private Excerpt excerpt;
    protected long threadId = -1;
    private String completeId;
    // final MongoManager manager;
    private String topoId;
    PlacementResponse pr = null;
    // private SimpleZookeeperClient szkc = null;
    private ZookeeperWatcher zw;
    Long timeout;
    Long advertise;
    ArrayList<Long> additionalThreads = new ArrayList<Long>();

    public void updateThreadId() {
	this.threadId = Thread.currentThread().getId();
	// Logging.append(completeId + " Received Thread ID " + threadId, Level.INFO, MonitorClient.class);
    }

    // private void loadExecutorsAndSetWatch() {
    //
    // Stat zkStat = new Stat();
    // szkc.getZK().getData("/storm/assignments" + "/" + topoId, new loadDataWatcher(), new AsyncCallback(), zkStat);
    // pr = manager.getDepl();
    //
    // }

    public void addThread(Long threadId) {
	additionalThreads.add(threadId);
    }

    public MonitorClient(String executorId, String topoid) {
	completeId = executorId + "@" + topoid;
	this.topoId = topoid;
	Logging.append("Starting Monitor Client for executor with id: " + completeId, Level.INFO, MonitorClient.class);

	Map confs = backtype.storm.utils.Utils.readStormConfig();
	timeout = ((Integer) confs.get(Config.TAOS_QUEUE_DELETE_TIMEOUT)).longValue();
	advertise = ((Integer) confs.get(Config.TAOS_CPU_TIME_ADVERTISE_PERIOD)).longValue();
	// String mongoIp = (String) confs.get(Config.TAOS_MONGO_DB_IP);
	// Integer mongoPort = (Integer) confs.get(Config.TAOS_MONGO_DB_PORT);
	// int ttl = (Integer) confs.get(Config.TAOS_DB_TTL);
	// Double updateDamping = (Double) confs.get(Config.TAOS_DAMPING_CONSTANT);

	// manager = new MongoManager(mongoIp, mongoPort, ttl, updateDamping.floatValue());
	// try {
	// Object obj = confs.get(Config.STORM_ZOOKEEPER_SERVERS);
	// Integer port = (Integer) confs.get(Config.STORM_ZOOKEEPER_PORT);
	// if (obj instanceof List) {
	// List<String> servers = (List<String>) obj;
	// szkc = new SimpleZookeeperClient(servers, port);
	// }
	// } catch (IOException e1) {
	// // TODO Auto-generated catch block
	// e1.printStackTrace();
	// }
	// loadExecutorsAndSetWatch();
	zw = new ZookeeperWatcher(topoId,false);
	synchronized (zw.getZoo()) {
	    pr = zw.getPr();
	}
	Thread updater = new Thread(new Runnable() {

	    @Override
	    public void run() {
		while (true) {
		    try {
			Thread.sleep(1000);
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		    pr = zw.getPr();

		}

	    }
	});
	updater.start();

	final String basePath = System.getProperty("java.io.tmpdir") + File.separator + "storm" + File.separator + "Chronicle" + File.separator
		+ completeId;
	// Delete if tempFile exists
	File fileTemp = new File(basePath + ".index");
	if (fileTemp.exists()) {
	    fileTemp.delete();
	}
	fileTemp = new File(basePath + ".data");
	if (fileTemp.exists()) {
	    fileTemp.delete();
	}

	ChronicleTools.deleteOnExit(basePath);
	try {
	    chronicle = new IndexedChronicle(basePath);
	} catch (IOException e) {
	    // TODO Auto-generated catch block

	    Logging.append(e.getMessage(), Level.INFO, this.getClass());
	}
	chronicle.useUnsafe(true);
	excerpt = chronicle.createExcerpt();

	scheduler.schedule(new AckerThread(), timeout / 3, TimeUnit.MILLISECONDS);
	scheduler.schedule(new CpuNotifThread(), advertise, TimeUnit.MILLISECONDS);
	// (use relative path for Unix systems)
	File f = new File(basePath + ".lock");
	// (works for both Windows and Linux)
	f.getParentFile().mkdirs();
	try {
	    f.createNewFile();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    Logging.append(Utils.stringStackTrace(e), Level.SEVERE, MonitorClient.class);
	}

	Logging.append("Completed initialization, Monitor Client for executor with id: " + completeId, Level.INFO, MonitorClient.class);
    }

    public void AdvertiseTuplereceivedClojure(Tuple tuple) {
	if (isClojureReporter()) {
	    AdvertiseTupleReceived(tuple);
	}
    }

    public void AdvertiseTupleReceived(Tuple tuple) {

	if (pr == null)
	    return;
	ExecutorTopo exect = pr.getExecutorByTask(tuple.getSourceTask(), topoId);

	if (exect == null) {
	    Logging.append("Source id " + tuple.getSourceTask() + " for received tuple not found, ignoring tuple", Level.INFO, MonitorClient.class);
	    pr = zw.getPr();
	    return;

	}
	// Logging.append("Advertising rcvd tuple, src " + exect.getMongoId() + " dest " + completeId, Level.INFO, MonitorClient.class);
	sendMessage(excerpt, Constants.TUPLE_NOTIFY + "_" + exect.getMongoId());
    }

    private synchronized void sendMessage(Excerpt e, String Msg) {
	e.startExcerpt(100);
	e.writeBytes(Msg);
	e.finish();
	lastMsg = System.currentTimeMillis();
    }

    class AckerThread implements Runnable {
	public void run() {
	    long delta = System.currentTimeMillis() - lastMsg;

	    if (delta >= timeout / 3) {
		sendMessage(excerpt, Constants.ACK);
		scheduler.schedule(new AckerThread(), timeout / 3, TimeUnit.MILLISECONDS);
	    } else {
		scheduler.schedule(new AckerThread(), timeout / 3 - delta, TimeUnit.MILLISECONDS);

	    }
	}

    }

    class CpuNotifThread implements Runnable {

	public void run() {
	    if (threadId != -1) {
		long total = thMxB.getThreadCpuTime(threadId);
		for (Long t : additionalThreads) {
		    total += thMxB.getThreadCpuTime(t);
		}
		sendMessage(excerpt, Constants.CPUTIME + "_" + total);

	    }

	    scheduler.schedule(new CpuNotifThread(), advertise, TimeUnit.MILLISECONDS);

	}
    }

    // class loadDataWatcher implements Watcher {
    // public void process(WatchedEvent event) {
    // Logging.append("Received event " + event.getType().name(), Level.INFO, MonitorClient.class);
    // Stat zkStat = new Stat();
    // szkc.getZK().getData("/storm/assignments" + "/" + topoId, new loadDataWatcher(), new AsyncCallback(), zkStat);
    // pr = manager.getDepl();
    // }
    // }

    // class AsyncCallback implements DataCallback {
    //
    // @Override
    // public void processResult(int arg0, String arg1, Object arg2, byte[] arg3, Stat arg4) {
    //
    // }
    //
    // }

    public boolean isClojureReporter() {
	return clojureReporter;
    }

    public void setClojureReporter(boolean clojureReporter) {
	this.clojureReporter = clojureReporter;
    }

}
