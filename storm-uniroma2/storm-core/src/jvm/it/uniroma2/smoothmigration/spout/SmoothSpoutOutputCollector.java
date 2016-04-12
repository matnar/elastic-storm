package it.uniroma2.smoothmigration.spout;

import it.uniroma2.smoothmigration.bolt.SmoothBoltWatcher;
import it.uniroma2.smoothmigration.dsm.ZookeeperWatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.utils.Utils;

public class SmoothSpoutOutputCollector {
    SpoutOutputCollector oc;
    SmoothBoltWatcher migraWatcher;
    ReentrantLock uniquelock;
    Condition notEmpty;
    ArrayList<SpoutCommand> tupleBuffer = new ArrayList<SpoutCommand>();
    HashMap<String, Integer> bufferByStream = new HashMap<String, Integer>();
    ArrayList<String> migStreams = new ArrayList<String>();
    ArrayList<String> newMigStreams = new ArrayList<String>();
    HashMap<String, Integer> seqnumEmitted = new HashMap<String, Integer>();
    private static final long OPENDELAY = 2000;
    boolean addSeqNum = false;
    boolean bufferMode = true;
    private long setFalseTimestamp = 0;

    public SmoothSpoutOutputCollector(SpoutOutputCollector oc, ReentrantLock lock) {
	super();
	this.oc = oc;
	uniquelock = lock;
	notEmpty = lock.newCondition();
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	Boolean SMOOTH_ENABLED = (Boolean) confs.get(Config.TAOS_SMOOTH_ENABLED);
	if (SMOOTH_ENABLED) {
	    Thread consumer = new Thread(new Consumer());
	    consumer.start();
	    migStreams.add("*");
	}

    }

    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
	if (migStreams.contains(streamId) || migStreams.contains("*")) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.STREAMID_TUPLE_MESSAGEID, streamId, messageId, -1, tuple));
	    notEmpty.signal();
	    return new ArrayList<Integer>();
	} else {
	    if (!bufferByStream.containsKey(streamId)) {
		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		List<Integer> dests = oc.emit(streamId, tuple, messageId);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}
		return dests;
	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.STREAMID_TUPLE_MESSAGEID, streamId, messageId, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}

    }

    public List<Integer> emit(List<Object> tuple) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.TUPLE, Utils.DEFAULT_STREAM_ID, null, -1, tuple));
	    notEmpty.signal();
	    return new ArrayList<Integer>();
	} else {
	    if (!bufferByStream.containsKey(Utils.DEFAULT_STREAM_ID)) {
		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		List<Integer> dests = oc.emit(tuple);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}

		return dests;
	    }

	    else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.TUPLE, Utils.DEFAULT_STREAM_ID, null, -1, tuple));
		notEmpty.signal();

		return new ArrayList<Integer>();
	    }
	}
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {

	if (migStreams.contains(streamId) || migStreams.contains("*")) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.STREAMID_TUPLE, streamId, null, -1, tuple));
	    notEmpty.signal();
	    return new ArrayList<Integer>();
	} else {
	    if (!bufferByStream.containsKey(streamId)) {

		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		List<Integer> dests = oc.emit(streamId, tuple);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}
		return dests;
	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.STREAMID_TUPLE, streamId, null, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}

    }

    public List<Integer> emit(List<Object> tuple, Object messageId) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.TUPLE_MESSAGEID, Utils.DEFAULT_STREAM_ID, messageId, -1, tuple));
	    notEmpty.signal();
	    return new ArrayList<Integer>();
	} else {
	    if (!bufferByStream.containsKey(Utils.DEFAULT_STREAM_ID)) {

		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		List<Integer> dests = oc.emit(tuple, messageId);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}
		return dests;
	    }

	    else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.TUPLE_MESSAGEID, Utils.DEFAULT_STREAM_ID, messageId, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {

	if (migStreams.contains(streamId) || migStreams.contains("*")) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_STREAMID_TUPLE_MESSAGEID, streamId, messageId, -1, tuple));
	    notEmpty.signal();
	} else {
	    if (!bufferByStream.containsKey(streamId)) {
		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		oc.emitDirect(taskId, streamId, tuple, messageId);
		if (addSeqNum) {
		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_STREAMID_TUPLE_MESSAGEID, streamId, messageId, -1, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, List<Object> tuple, Object messageId) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_TUPLE_MESSAGEID, Utils.DEFAULT_STREAM_ID, messageId, -1, tuple));
	    notEmpty.signal();
	} else {
	    if (!bufferByStream.containsKey(Utils.DEFAULT_STREAM_ID)) {
		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		oc.emitDirect(taskId, tuple, messageId);
		if (addSeqNum) {
		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_TUPLE_MESSAGEID, Utils.DEFAULT_STREAM_ID, messageId, -1, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {

	if (migStreams.contains(streamId) || migStreams.contains("*")) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_STREAMID_TUPLE, streamId, null, -1, tuple));
	    notEmpty.signal();
	} else {
	    if (!bufferByStream.containsKey(streamId)) {
		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		oc.emitDirect(taskId, streamId, tuple);
		if (addSeqNum) {
		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_STREAMID_TUPLE, streamId, null, -1, tuple));
		notEmpty.signal();

	    }
	}
    }

    public void emitDirect(int taskId, List<Object> tuple) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_TUPLE, Utils.DEFAULT_STREAM_ID, null, -1, tuple));
	    notEmpty.signal();

	} else {
	    if (!bufferByStream.containsKey(Utils.DEFAULT_STREAM_ID)) {
		if (addSeqNum) {
		    HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		    for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
			toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		    }
		    tuple.add(toAdd);
		}
		oc.emitDirect(taskId, tuple);
		if (addSeqNum) {
		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new SpoutCommand(SpoutCommand.TASKID_TUPLE, Utils.DEFAULT_STREAM_ID, null, -1, tuple));
		notEmpty.signal();

	    }
	}
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

    private void decrementOrDelete(String key) {

	Integer size = bufferByStream.get(key);
	if (size != null) {
	    size--;
	    bufferByStream.put(key, size);
	}
	if (size == 0)
	    bufferByStream.remove(key);

    }

    public void runACommand() {
	SpoutCommand toExecute = tupleBuffer.get(0);
	tupleBuffer.remove(0);
	String type = toExecute.getCommand_type();
	if (type.equals(SpoutCommand.TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    List<Integer> dests = oc.emit(toExecute.getTuple());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(SpoutCommand.TUPLE_MESSAGEID)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    List<Integer> dests = oc.emit(toExecute.getTuple(), toExecute.getMessageId());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(SpoutCommand.STREAMID_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    List<Integer> dests = oc.emit(toExecute.getStreamId(), toExecute.getTuple());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(SpoutCommand.STREAMID_TUPLE_MESSAGEID)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    List<Integer> dests = oc.emit(toExecute.getStreamId(), toExecute.getTuple(), toExecute.getMessageId());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(SpoutCommand.TASKID_STREAMID_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    oc.emitDirect(toExecute.getTaskId(), toExecute.getStreamId(), toExecute.getTuple());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);

	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(SpoutCommand.TASKID_STREAMID_TUPLE_MESSAGEID)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    oc.emitDirect(toExecute.getTaskId(), toExecute.getStreamId(), toExecute.getTuple(), toExecute.getMessageId());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);

	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(SpoutCommand.TASKID_TUPLE)) {

	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    oc.emitDirect(toExecute.getTaskId(), toExecute.getTuple());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);

	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(SpoutCommand.TASKID_TUPLE_MESSAGEID)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    oc.emitDirect(toExecute.getTaskId(), toExecute.getTuple(), toExecute.getMessageId());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);

	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	}
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
		uniquelock.lock();
		if (tupleBuffer.size() == 0 || bufferMode)
		    try {
			notEmpty.await(10, TimeUnit.SECONDS);
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		if (System.currentTimeMillis() - setFalseTimestamp > OPENDELAY && setFalseTimestamp<ZookeeperWatcher.getLastRefreshTime()) {
		    while (tupleBuffer.size() != 0 && !bufferMode) {
			runACommand();
			try {
			    Thread.sleep(0, 500);
			} catch (InterruptedException e) {
			    // TODO Auto-generated catch block
			    e.printStackTrace();
			}
		    }
		}
		uniquelock.unlock();
		try {
		    Thread.sleep(1000);
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

	    }

	}

    }

    public HashMap<String, Integer> getSeqnumEmitted() {
	return seqnumEmitted;
    }

    public void setSeqnumEmitted(HashMap<String, Integer> seqnumEmitted) {
	this.seqnumEmitted = seqnumEmitted;
    }

    public void setMigStreams(ArrayList<String> migStreams) {
	if (System.currentTimeMillis() - setFalseTimestamp > OPENDELAY && setFalseTimestamp<ZookeeperWatcher.getLastRefreshTime()) {
	    this.migStreams = migStreams;
	}
    }

    public SpoutOutputCollector getOc() {
	return oc;
    }

    public void setOc(SpoutOutputCollector oc) {
	this.oc = oc;
    }

    public boolean isBufferMode() {
	return bufferMode;
    }

    public void setBufferMode(boolean bufferMode) {
	boolean oldvalue = this.bufferMode;
	this.bufferMode = bufferMode;
	if (oldvalue != bufferMode) {
	    notEmpty.signal();
	    if (bufferMode == false) {
		setFalseTimestamp = System.currentTimeMillis();
	    }
	}

    }

    public boolean isAddSeqNum() {
	return addSeqNum;
    }

    public void setAddSeqNum(boolean addSeqNum) {
	this.addSeqNum = addSeqNum;
    }

    public ArrayList<SpoutCommand> getTupleBuffer() {
	return tupleBuffer;
    }

}
