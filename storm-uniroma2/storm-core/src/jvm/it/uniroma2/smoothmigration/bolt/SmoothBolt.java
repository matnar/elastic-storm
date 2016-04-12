package it.uniroma2.smoothmigration.bolt;

import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import it.uniroma2.taos.monitor.MonitorClient;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.network.TaskTopo;
import it.uniroma2.taos.commons.persistence.MongoManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;

import com.hazelcast.config.SerializerConfig;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class SmoothBolt extends BaseRichBolt {
    private boolean skip = true;
    private Thread migrationHandler = null;
    private ArrayList<Tuple> inBuffer = new ArrayList<Tuple>();
    private TopologyContext topologyContext = null;
    private Date lastTupleTime = new Date();
    private Date lastMigratingSourceTime = new Date();
    private ArrayList<TaskTopo> EOFS = new ArrayList<TaskTopo>();
    private ReentrantLock lock = new ReentrantLock(true);
    private Condition notEmpty;
    private HashMap<String, Integer> inSeqNum = new HashMap<String, Integer>();
    private SmoothOutputCollector soc;
    public static final String SEQNUM_FIELD_NAME = "seqnum";
    MonitorClient mc = null;
    private boolean SMOOTH_ENABLED = true;
    private SmoothBoltWatcher sbw;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	SMOOTH_ENABLED = (Boolean) confs.get(Config.TAOS_SMOOTH_ENABLED);
	notEmpty = lock.newCondition();
	mc = topologyContext.getMc();
	mc.setClojureReporter(false);
	soc = new SmoothOutputCollector(outputCollector, lock, topologyContext.getThisTaskId(), SMOOTH_ENABLED,this);
	if (!SMOOTH_ENABLED) {
	    String mongoIp = (String) confs.get(Config.TAOS_MONGO_DB_IP);
	    Integer mongoPort = (Integer) confs.get(Config.TAOS_MONGO_DB_PORT);
	    int ttl = (Integer) confs.get(Config.TAOS_DB_TTL);
	    Double updateDamping = (Double) confs.get(Config.TAOS_METRICS_SMOOTHING);
	    MongoManager manager = new MongoManager(mongoIp, mongoPort, ttl, updateDamping.floatValue());
	    manager.putRdyTask(topologyContext.getThisTaskId());
	    skip = false;
	    soc.setBufferMode(false);
	}
	this.topologyContext = topologyContext;
	sbw = null;
	if (SMOOTH_ENABLED)
	    sbw = new SmoothBoltWatcher(this, soc);
	smoothprepare(map, topologyContext, soc);
	if (SMOOTH_ENABLED) {
	    if (migrationHandler == null) {
		migrationHandler = new Thread(sbw);
		migrationHandler.start();
	    }
	    Thread t = new Thread(new Consumer());
	    t.start();
	    mc.addThread(t.getId());
	}

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
	outputFieldsDeclarer.declareStream("eofs", new Fields("eoffield"));
	smoothDeclareOutputFields(outputFieldsDeclarer);
    }

    public void execute(Tuple tuple) {
	lock.lock();
	if (filterEOFtuple(tuple)) {
	    synchronized (EOFS) {
		TaskTopo toadd = new TaskTopo(tuple.getSourceTask(), topologyContext.getStormId());
		Logging.append(topologyContext.getThisTaskId() + "@" + topologyContext.getThisComponentId() + " received eof: " + toadd.toString(),
			Level.INFO, this.getClass());
		if (!EOFS.contains(toadd)) {
		    EOFS.add(toadd);
		    if (sbw != null) {
			sbw.updateTimeout();
		    }
		}

	    }
	} else {
	    int id = tuple.getSourceTask();
	    for (TaskTopo taskTopo : sbw.getMigratingTasks()) {
		if (taskTopo.getTaskId() == id)
		    sbw.updateTimeout();
	    }
	    if (skip) {
		inBuffer.add(tuple);
		notEmpty.signal();
	    } else {
		if (inBuffer.size() != 0) {
		    if (!checkSeqNum(inBuffer.get(0))) {
			inBuffer.remove(0);
			inBuffer.add(tuple);
			notEmpty.signal();
			lock.unlock();
			return;
		    }
		    mc.AdvertiseTupleReceived(inBuffer.get(0));
		    smoothedExecute(inBuffer.get(0));
		    inBuffer.remove(0);
		    inBuffer.add(tuple);
		    notEmpty.signal();
		} else {
		    if (!checkSeqNum(tuple)) {
			lock.unlock();
			return;
		    }
		    mc.AdvertiseTupleReceived(tuple);
		    smoothedExecute(tuple);

		}
	    }
	}
	lock.unlock();
    }

    private int addOrIncrement(String key, HashMap<String, Integer> target) {

	Integer size = target.get(key);
	if (size != null) {
	    size++;
	    target.put(key, size);
	    return size;
	} else {
	    target.put(key, 1);
	    return 1;
	}

    }

    private boolean checkSeqNum(Tuple tuple) {
	if (!SMOOTH_ENABLED)
	    return true;
	if (tuple.getFields().contains(SEQNUM_FIELD_NAME) && soc.isaddSeqNum()) {
	    HashMap<String, Integer> seqnum = (HashMap<String, Integer>) tuple.getValueByField(SEQNUM_FIELD_NAME);
	    Integer num = seqnum.get(this.getTopologyContext().getThisTaskId() + "");
	    if (num == null) {
		num = 0;
	    }
	    Integer expected = inSeqNum.get(tuple.getSourceTask() + "");
	    if (expected == null) {
		expected = 0;
	    }
	    if (expected > num) {
		Logging.append(topologyContext.getThisTaskId() + "@" + topologyContext.getStormId() + "REPEATED expected: " + expected
			+ " received: " + num + " srcTask: " + tuple.getSourceTask(), Level.INFO, this.getClass());
		return false;
	    } else if (expected < num) {
		Logging.append(topologyContext.getThisTaskId() + "@" + topologyContext.getStormId() + " ERROR SEQNUM expected: " + expected
			+ " received: " + num + " srcTask: " + tuple.getSourceTask(), Level.INFO, this.getClass());
		Logging.append(topologyContext.getThisTaskId() + "@" + topologyContext.getStormId() + " SEQNUM " + inSeqNum.toString(), Level.INFO,
			this.getClass());
		inSeqNum.put(tuple.getSourceTask() + "", num + 1);
		return true;
	    }

	    addOrIncrement(tuple.getSourceTask() + "", inSeqNum);
	    return true;
	}
	return true;

    }

    private boolean filterEOFtuple(Tuple tuple) {
	if (tuple.getSourceStreamId().equals("eofs")) {
	    return true;
	}
	return false;
    }

    class Consumer implements Runnable {

	@Override
	public void run() {
	    try {
		Thread.sleep(10000);
	    } catch (InterruptedException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	    }
	    while (true) {
		lock.lock();
		if (inBuffer.size() == 0)
		    try {
			notEmpty.await(10, TimeUnit.SECONDS);
		    } catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		if (inBuffer.size() != 0 && !skip) {
		    if (!checkSeqNum(inBuffer.get(0))) {
			inBuffer.remove(0);
			lock.unlock();
			continue;
		    }
		    mc.AdvertiseTupleReceived(inBuffer.get(0));
		    smoothedExecute(inBuffer.get(0));
		    inBuffer.remove(0);
		    lock.unlock();
		} else {
		    lock.unlock();
		    try {
			Thread.sleep(500);
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		}
	    }

	}
    }

    public boolean isSkip() {
	return skip;
    }

    public void setSkip(boolean skip) {
	this.skip = skip;

    }

    public ArrayList<Tuple> getInBuffer() {
	return inBuffer;
    }

    public void setInBuffer(ArrayList<Tuple> inBuffer) {
	this.inBuffer = inBuffer;
    }

    public TopologyContext getTopologyContext() {
	return topologyContext;
    }

    public void setTopologyContext(TopologyContext tc) {
	this.topologyContext = tc;
    }

    public Date getLastTupleTime() {
	return lastTupleTime;
    }

    public void setLastTupleTime(Date lastTuple) {
	this.lastTupleTime = lastTuple;
    }

    public Date getLastMigratingSourceTime() {
	return lastMigratingSourceTime;
    }

    public void setLastMigratingSourceTime(Date lastMigratingSourceTime) {
	this.lastMigratingSourceTime = lastMigratingSourceTime;
    }

    public ArrayList<TaskTopo> getEOFS() {

	ArrayList<TaskTopo> toReturn = new ArrayList<TaskTopo>();
	synchronized (EOFS) {
	    toReturn.addAll(EOFS);
	    return toReturn;
	}
    }

    public void clearEOFS() {
	synchronized (EOFS) {
	    EOFS.clear();
	}
    }

    public HashMap<String, Integer> getInSeqNum() {
	return inSeqNum;
    }

    public void setInSeqNum(HashMap<String, Integer> inSeqNum) {
	this.inSeqNum = inSeqNum;
    }

    public Lock getLock() {
	return lock;
    }

    public void setEOFS(ArrayList<TaskTopo> eOFS) {
	EOFS = eOFS;
    }

    public boolean isSMOOTH_ENABLED() {
	return SMOOTH_ENABLED;
    }

    public void setSMOOTH_ENABLED(boolean sMOOTH_ENABLED) {
	SMOOTH_ENABLED = sMOOTH_ENABLED;
    }

    public SmoothBoltWatcher getSbw() {
        return sbw;
    }
    public abstract void smoothDeclareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    public abstract void smoothprepare(Map map, TopologyContext topologyContext, SmoothOutputCollector outputCollector);

    public abstract void smoothedExecute(Tuple tuple);

    public abstract HashMap<String, Object> getState();

    public abstract void setState(HashMap<String, Object> state);

    public abstract ArrayList<SerializerConfig> getSerializers();
}
