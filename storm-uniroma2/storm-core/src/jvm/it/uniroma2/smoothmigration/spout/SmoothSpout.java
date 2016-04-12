package it.uniroma2.smoothmigration.spout;

import it.uniroma2.taos.commons.persistence.MongoManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.hazelcast.config.SerializerConfig;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public abstract class SmoothSpout extends BaseRichSpout {
    TopologyContext topologyContext;
    SmoothSpoutOutputCollector oc;
    Thread watcher = null;
    boolean enabled = false;
    private ReentrantLock lock = new ReentrantLock(true);
    private Boolean SMOOTH_ENABLED;
    public static final String SEQNUM_FIELD_NAME = "seqnum";

    public Lock getLock() {
	return lock;
    }

    public boolean isEnabled() {
	return enabled;
    }

    public void setEnabled(boolean enabled) {
	this.enabled = enabled;

    }

    @Override
    public void nextTuple() {

	lock.lock();
	if (enabled)
	    // synchronized (oc.getOc()) {
	    smoothNextTuple();
	// }
	lock.unlock();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declareStream("eofs", new Fields("eoffield"));
	smoothDeclareOutputFields(declarer);
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector collector) {
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	SMOOTH_ENABLED = (Boolean) confs.get(Config.TAOS_SMOOTH_ENABLED);
	this.topologyContext = topologyContext;
	oc = new SmoothSpoutOutputCollector(collector, lock);
	if (SMOOTH_ENABLED) {
	    SmoothSpoutWatcher spw = new SmoothSpoutWatcher(this, oc);
	    if (watcher == null) {
		watcher = new Thread(spw);
		watcher.start();
	    }
	} else {
	    String mongoIp = (String) confs.get(Config.TAOS_MONGO_DB_IP);
	    Integer mongoPort = (Integer) confs.get(Config.TAOS_MONGO_DB_PORT);
	    int ttl = (Integer) confs.get(Config.TAOS_DB_TTL);
	    Double updateDamping = (Double) confs.get(Config.TAOS_METRICS_SMOOTHING);
	    MongoManager manager = new MongoManager(mongoIp, mongoPort, ttl, updateDamping.floatValue());
	    manager.putRdyTask(topologyContext.getThisTaskId());
	    enabled = true;
	    lock.lock();
	    oc.setBufferMode(false);
	    lock.unlock();
	}

	smoothOpen(conf, topologyContext, oc);
    }

    public TopologyContext getTopologyContext() {
	return topologyContext;
    }

    public void setTopologyContext(TopologyContext topologyContext) {
	this.topologyContext = topologyContext;
    }

    public SmoothSpoutOutputCollector getOc() {
	return oc;
    }

    public void setOc(SmoothSpoutOutputCollector oc) {
	this.oc = oc;
    }

    public abstract void smoothDeclareOutputFields(OutputFieldsDeclarer declarer);

    public abstract void smoothNextTuple();

    public abstract void smoothOpen(Map conf, TopologyContext topologyContext, SmoothSpoutOutputCollector collector);

    public abstract HashMap<String, Object> getState();

    public abstract void setState(HashMap<String, Object> state);

    public abstract ArrayList<SerializerConfig> getSerializers();
}
