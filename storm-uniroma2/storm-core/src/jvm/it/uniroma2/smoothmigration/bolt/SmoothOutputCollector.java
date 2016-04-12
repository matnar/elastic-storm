package it.uniroma2.smoothmigration.bolt;

import it.uniroma2.smoothmigration.dsm.ZookeeperWatcher;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.network.Assignment;
import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.network.TaskTopo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import ch.qos.logback.core.pattern.util.AsIsEscapeUtil;
import backtype.storm.Config;
import backtype.storm.messaging.netty.Client;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class SmoothOutputCollector {
    OutputCollector oc;
    SmoothBolt sb;
    ArrayList<Command> tupleBuffer = new ArrayList<Command>();
    HashMap<String, Integer> bufferByStream = new HashMap<String, Integer>();
    ArrayList<String> migStreams = new ArrayList<String>();
    boolean bufferMode = true;
    HashMap<String, Integer> seqnumEmitted = new HashMap<String, Integer>();
    boolean addSeqNum = false;
    int srcTsk = 0;
    private ReentrantLock uniqueLock;
    Condition notEmpty;
    long setFalseTimestamp = 0;
    boolean debug = false;
    private static final long OPENDELAY = 2000;

    public SmoothOutputCollector(OutputCollector oc, ReentrantLock lock, int srcTask, boolean smoothEnabled, SmoothBolt sb) {
	super();
	this.sb = sb;
	uniqueLock = lock;
	notEmpty = lock.newCondition();
	this.srcTsk = srcTask;
	this.oc = oc;
	if (smoothEnabled) {
	    Thread consumer = new Thread(new Consumer());
	    consumer.start();
	}

    }


    public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {

	if (migStreams.contains(streamId)) {
	    addOrIncrement(streamId, bufferByStream);
	    tupleBuffer.add(new Command(Command.STREAMID_ANCHORS_TUPLE, streamId, anchors, -1, tuple));
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

		List<Integer> dests = oc.emit(streamId, anchors, tuple);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}

		return dests;
	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new Command(Command.STREAMID_ANCHORS_TUPLE, streamId, anchors, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}

    }

    public List<Integer> emit(String streamId, Tuple anchor, List<Object> tuple) {

	ArrayList<Tuple> anchors = new ArrayList<Tuple>();
	anchors.add(anchor);
	if (migStreams.contains(streamId)) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new Command(Command.STREAMID_ANCHOR_TUPLE, streamId, anchors, -1, tuple));
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
		// if (counter > 0) {
		// counter--;
		// System.out.println("srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());
		// }
		List<Integer> dests = oc.emit(streamId, anchor, tuple);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}

		return dests;
	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new Command(Command.STREAMID_ANCHOR_TUPLE, streamId, anchors, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}
    }

    public List<Integer> emit(String streamId, List<Object> tuple) {

	if (migStreams.contains(streamId)) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new Command(Command.STREAMID_TUPLE, streamId, null, -1, tuple));
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

		tupleBuffer.add(new Command(Command.STREAMID_TUPLE, streamId, null, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}

    }

    public List<Integer> emit(Collection<Tuple> anchors, List<Object> tuple) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new Command(Command.ANCHORS_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, -1, tuple));
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

		List<Integer> dests = oc.emit(anchors, tuple);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}

		return dests;
	    }

	    else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new Command(Command.ANCHORS_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}
    }

    public List<Integer> emit(Tuple anchor, List<Object> tuple) {

	ArrayList<Tuple> anchors = new ArrayList<Tuple>();
	anchors.add(anchor);
	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new Command(Command.ANCHOR_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, -1, tuple));
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
		// if (counter > 0) {
		// counter--;
		// System.out.println("srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());
		// }
		List<Integer> dests = oc.emit(anchor, tuple);
		if (addSeqNum) {
		    for (Integer task : dests) {
			addOrIncrement(task.toString(), seqnumEmitted);
		    }
		}

		return dests;
	    } else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new Command(Command.ANCHOR_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}
    }

    public List<Integer> emit(List<Object> tuple) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new Command(Command.TUPLE, Utils.DEFAULT_STREAM_ID, null, -1, tuple));
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

		tupleBuffer.add(new Command(Command.TUPLE, Utils.DEFAULT_STREAM_ID, null, -1, tuple));
		notEmpty.signal();
		return new ArrayList<Integer>();
	    }
	}
    }

    public void emitDirect(int taskId, String streamId, Tuple anchor, List<Object> tuple) {

	ArrayList<Tuple> anchors = new ArrayList<Tuple>();
	anchors.add(anchor);
	if (migStreams.contains(streamId)) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new Command(Command.TASKID_STREAMID_ANCHOR_TUPLE, streamId, anchors, taskId, tuple));
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

		oc.emitDirect(taskId, streamId, anchor, tuple);
		if (addSeqNum) {

		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new Command(Command.TASKID_STREAMID_ANCHOR_TUPLE, streamId, anchors, taskId, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, String streamId, List<Object> tuple) {

	if (migStreams.contains(streamId)) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new Command(Command.TASKID_STREAMID_TUPLE, streamId, null, taskId, tuple));
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
		// if (counter > 0) {
		// counter--;
		// System.out.println("srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());
		// }
		oc.emitDirect(taskId, streamId, tuple);
		if (addSeqNum) {
		    addOrIncrement(taskId + "", seqnumEmitted);
		}

	    }

	    else {
		addOrIncrement(streamId, bufferByStream);

		tupleBuffer.add(new Command(Command.TASKID_STREAMID_TUPLE, streamId, null, taskId, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, Collection<Tuple> anchors, List<Object> tuple) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new Command(Command.TASKID_ANCHORS_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, taskId, tuple));
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
		// if (counter > 0) {
		// counter--;
		// System.out.println("srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());
		// }
		oc.emitDirect(taskId, anchors, tuple);
		if (addSeqNum) {

		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

		tupleBuffer.add(new Command(Command.TASKID_ANCHORS_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, taskId, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, Tuple anchor, List<Object> tuple) {
	ArrayList<Tuple> anchors = new ArrayList<Tuple>();
	anchors.add(anchor);
	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);
	    tupleBuffer.add(new Command(Command.TASKID_ANCHOR_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, taskId, tuple));
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

		oc.emitDirect(taskId, anchor, tuple);
		if (addSeqNum) {
		    addOrIncrement(taskId + "", seqnumEmitted);
		}
	    } else {
		addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);
		tupleBuffer.add(new Command(Command.TASKID_ANCHOR_TUPLE, Utils.DEFAULT_STREAM_ID, anchors, taskId, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, List<Object> tuple) {

	if (migStreams.contains(Utils.DEFAULT_STREAM_ID)) {
	    addOrIncrement(Utils.DEFAULT_STREAM_ID, bufferByStream);

	    tupleBuffer.add(new Command(Command.TASKID_TUPLE, Utils.DEFAULT_STREAM_ID, null, taskId, tuple));
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

		tupleBuffer.add(new Command(Command.TASKID_TUPLE, Utils.DEFAULT_STREAM_ID, null, taskId, tuple));
		notEmpty.signal();
	    }
	}
    }

    public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors, List<Object> tuple) {

	if (migStreams.contains(streamId)) {
	    addOrIncrement(streamId, bufferByStream);

	    tupleBuffer.add(new Command(Command.TASKID_STREAMID_ANCHORS_TUPLE, streamId, anchors, taskId, tuple));
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
		// if (counter > 0) {
		// counter--;
		// System.out.println("srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());
		// }
		oc.emitDirect(taskId, streamId, anchors, tuple);
		if (addSeqNum) {

		    addOrIncrement(taskId + "", seqnumEmitted);

		}

	    }

	    else {
		addOrIncrement(streamId, bufferByStream);
		tupleBuffer.add(new Command(Command.TASKID_STREAMID_ANCHORS_TUPLE, streamId, anchors, taskId, tuple));
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

    public void runAcommand() {
	Command toExecute = tupleBuffer.get(0);
	tupleBuffer.remove(0);
	String type = toExecute.getCommand_type();
	if (type.equals(Command.TUPLE)) {
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
	} else if (type.equals(Command.ANCHOR_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    List<Integer> dests = oc.emit(toExecute.getAnchors().iterator().next(), toExecute.getTuple());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(Command.ANCHORS_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    List<Integer> dests = oc.emit(toExecute.getAnchors(), toExecute.getTuple());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(Command.STREAMID_TUPLE)) {
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
	} else if (type.equals(Command.STREAMID_ANCHOR_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    List<Integer> dests = oc.emit(toExecute.getStreamId(), toExecute.getAnchors().iterator().next(), toExecute.getTuple());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(Command.STREAMID_ANCHORS_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    List<Integer> dests = oc.emit(toExecute.getStreamId(), toExecute.getAnchors(), toExecute.getTuple());
	    if (addSeqNum) {
		for (Integer task : dests) {
		    addOrIncrement(task.toString(), seqnumEmitted);
		}
	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(Command.TASKID_ANCHOR_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }
	    oc.emitDirect(toExecute.getTaskId(), toExecute.getAnchors().iterator().next(), toExecute.getTuple());

	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);
	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(Command.TASKID_ANCHORS_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    // System.out.println("buffer srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());

	    oc.emitDirect(toExecute.getTaskId(), toExecute.getAnchors(), toExecute.getTuple());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);
	    }
	    decrementOrDelete(Utils.DEFAULT_STREAM_ID);
	} else if (type.equals(Command.TASKID_STREAMID_ANCHOR_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    oc.emitDirect(toExecute.getTaskId(), toExecute.getStreamId(), toExecute.getAnchors().iterator().next(), toExecute.getTuple());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);
	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(Command.TASKID_STREAMID_ANCHORS_TUPLE)) {
	    if (addSeqNum) {
		HashMap<String, Integer> toAdd = new HashMap<String, Integer>();
		for (Entry<String, Integer> entry : seqnumEmitted.entrySet()) {
		    toAdd.put(entry.getKey().toString(), new Integer(entry.getValue()));
		}
		toExecute.getTuple().add(toAdd);
	    }

	    oc.emitDirect(toExecute.getTaskId(), toExecute.getStreamId(), toExecute.getAnchors(), toExecute.getTuple());
	    if (addSeqNum) {
		addOrIncrement(toExecute.getTaskId() + "", seqnumEmitted);
	    }
	    decrementOrDelete(toExecute.getStreamId());
	} else if (type.equals(Command.TASKID_STREAMID_TUPLE)) {
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
	} else if (type.equals(Command.TASKID_TUPLE)) {
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
	}
    }

    class Consumer implements Runnable {

	@Override
	public void run() {
	    while (true) {
		uniqueLock.lock();
		if (tupleBuffer.size() == 0 || bufferMode)
		    try {
			notEmpty.await(10, TimeUnit.SECONDS);
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		if (System.currentTimeMillis() - setFalseTimestamp > OPENDELAY && setFalseTimestamp<ZookeeperWatcher.getLastRefreshTime()) {
		    
		    while (tupleBuffer.size() != 0 && !bufferMode) {
			runAcommand();
			try {
			    Thread.sleep(0, 500);
			} catch (InterruptedException e) {
			    // TODO Auto-generated catch block
			    e.printStackTrace();
			}
		    }
		}
		uniqueLock.unlock();
		try {
		    Thread.sleep(1000);
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}

	    }

	}

    }

    public void setMigStreams(ArrayList<String> migStreams) {
	if (System.currentTimeMillis() - setFalseTimestamp > OPENDELAY && setFalseTimestamp<ZookeeperWatcher.getLastRefreshTime()) {
	    this.migStreams = migStreams;
	}
    }

    public OutputCollector getOc() {
	return oc;
    }

    public boolean isBufferMode() {
	return bufferMode;
    }

    public void setBufferMode(boolean bufferMode) {

	boolean oldvalue = this.bufferMode;
	if (oldvalue != bufferMode) {
	    uniqueLock.lock();
	    this.bufferMode = bufferMode;
	    notEmpty.signal();
	    if (bufferMode == false) {
		setFalseTimestamp = System.currentTimeMillis();
		debug = true;
	    }
	    System.out.println("buffer srcTsk:" + srcTsk + " seqnumEmitted: " + seqnumEmitted.toString());
	    uniqueLock.unlock();
	}

    }

    public ArrayList<Command> getTupleBuffer() {
	return tupleBuffer;
    }

    public boolean isaddSeqNum() {
	return addSeqNum;
    }

    public void setaddSeqNum(boolean addSeqNum) {
	this.addSeqNum = addSeqNum;
    }

    public void setSeqnumEmitted(HashMap<String, Integer> seqnumEmitted) {
	this.seqnumEmitted = seqnumEmitted;
    }

    public HashMap<String, Integer> getSeqnumEmitted() {
	return seqnumEmitted;
    }

    public void ack(Tuple input) {
	oc.ack(input);

    }

    public void fail(Tuple input) {
	oc.fail(input);
    }

}
