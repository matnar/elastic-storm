package it.uniroma2.adaptivescheduler;

import it.uniroma2.adaptivescheduler.persistence.DatabaseException;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.persistence.entities.Measurement;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.Config;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class WorkerMonitor {
    /* Dario */
    private boolean enabled = false;

    /* Referring to the topology */
    private String stormId;

    /* Referring to the supervisor */
    private String assignmentId;

    /* Worker id */
    private String workerId;
    private int port;

    private DatabaseManager databaseManager = null;

    private Lock outgoingTuplesPerTaskLock;
    private Map<Long, Map<Long, Integer>> outgoingTuplesPerTask;

    private Lock incomingTuplesPerTaskLock;
    private Map<Long, Map<Long, Integer>> incomingTuplesPerTask;

    @SuppressWarnings("rawtypes")
    private Map config = null;

    private long latestStatComputationTimestamp = 0;

    public WorkerMonitor(String stormId, String assignmentId, String workerId, int port) {
	this.stormId = stormId;
	this.assignmentId = assignmentId;
	this.workerId = workerId;
	this.port = port;

	outgoingTuplesPerTaskLock = new ReentrantLock();
	incomingTuplesPerTaskLock = new ReentrantLock();
	this.outgoingTuplesPerTask = new HashMap<Long, Map<Long, Integer>>();
	this.incomingTuplesPerTask = new HashMap<Long, Map<Long, Integer>>();
    }

    public void initialize() throws ClassNotFoundException {

	/* Read configuration */
	config = Utils.readStormConfig();

	/* Create database */
	databaseManager = new DatabaseManager(getDatabasePath(), assignmentId);
	enabled = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_ENABLED);
	try {
	    Integer i = (Integer) config.get(Config.ADAPTIVE_SCHEDULER_INTERNAL_DATABASE_PORT);
	    if (enabled)
		databaseManager.initialize(i.intValue());
	} catch (DatabaseException e) {
	    e.printStackTrace();
	} catch (SQLException e) {
	    e.printStackTrace();
	}

	/* Update timestap last measurement */
	latestStatComputationTimestamp = System.currentTimeMillis();

    }

    public void stop() {

	/* Stopping Worker Monitor */

	/* Cleanup in-memory buffer */
	incomingTuplesPerTaskLock.lock();
	try {
	    incomingTuplesPerTask.clear();
	} finally {
	    incomingTuplesPerTaskLock.unlock();
	}

	outgoingTuplesPerTaskLock.lock();
	try {
	    outgoingTuplesPerTask.clear();
	} finally {
	    outgoingTuplesPerTaskLock.unlock();
	}

    }

    private String getDatabasePath() {
	String dbBaseDir = "./";

	if (config != null) {
	    dbBaseDir = (String) config.get(Config.STORM_LOCAL_DIR);

	    if (dbBaseDir != null) {
		if (!dbBaseDir.startsWith("/"))
		    dbBaseDir = "./" + dbBaseDir;

		if (!dbBaseDir.endsWith("/"))
		    dbBaseDir += "/";

	    } else {
		dbBaseDir = "./";
	    }
	}

	System.out.println("Base Dir: " + dbBaseDir);
	return dbBaseDir;
    }

    public void computeStats() {

	if (!enabled)
	    return;
	long now = System.currentTimeMillis();

	averageEmittedTuplePerTask((now - latestStatComputationTimestamp));
	averageReceivedTuplePerTask((now - latestStatComputationTimestamp));

	/* Update timestamp of the latest computation */
	latestStatComputationTimestamp = now;

    }

    private void averageReceivedTuplePerTask(long delta) {

	incomingTuplesPerTaskLock.lock();

	try {
	    Set<Long> fromTasks = incomingTuplesPerTask.keySet();
	    for (Long fromTaskId : fromTasks) {

		Map<Long, Integer> toTaskTuplesReceived = incomingTuplesPerTask.get(fromTaskId);
		if (toTaskTuplesReceived == null)
		    continue;

		Set<Long> toTasks = toTaskTuplesReceived.keySet();
		for (Long toTaskId : toTasks) {

		    Integer count = toTaskTuplesReceived.get(toTaskId);

		    if (count == null)
			continue;

		    /* Compute average and save it into database */
		    double avg = 0.0;
		    if (delta > 0.0)
			avg = ((double) count.intValue()) / ((double) delta / 1000.0);

		    try {
			Measurement m = new Measurement(fromTaskId.toString(), toTaskId.toString(), avg, stormId, assignmentId, workerId, port);

			databaseManager.saveMeasurement(m, true);

		    } catch (SQLException e) {
			e.printStackTrace();
		    } catch (DatabaseException e) {
			e.printStackTrace();
		    }
		}
	    }

	    /* Cleanup in-memory buffer */
	    incomingTuplesPerTask.clear();

	} finally {
	    incomingTuplesPerTaskLock.unlock();
	}

    }

    private void averageEmittedTuplePerTask(long delta) {

	outgoingTuplesPerTaskLock.lock();
	try {
	    Set<Long> fromTasks = outgoingTuplesPerTask.keySet();
	    for (Long fromTaskId : fromTasks) {

		Map<Long, Integer> toTaskTuplesEmitted = outgoingTuplesPerTask.get(fromTaskId);

		if (toTaskTuplesEmitted == null)
		    continue;

		Set<Long> toTasks = toTaskTuplesEmitted.keySet();
		for (Long toTaskId : toTasks) {

		    Integer count = toTaskTuplesEmitted.get(toTaskId);

		    if (count == null)
			continue;

		    /* Compute average and save it into database */
		    double avg = 0.0;
		    if (delta > 0.0)
			avg = ((double) count.intValue()) / ((double) delta / 1000.0);

		    try {
			Measurement m = new Measurement(fromTaskId.toString(), toTaskId.toString(), avg, stormId, assignmentId, workerId, port);

			databaseManager.saveMeasurement(m, true);

		    } catch (SQLException e) {
			e.printStackTrace();
		    } catch (DatabaseException e) {
			e.printStackTrace();
		    }
		}
	    }

	    /* Cleanup in-memory buffer */
	    outgoingTuplesPerTask.clear();

	} finally {
	    outgoingTuplesPerTaskLock.unlock();
	}
    }

    public void notifyOutgoingTuple(long outTask, Tuple tuple) {

	Long fromTaskId = (long) tuple.getSourceTask();
	Long toTaskId = new Long(outTask);

	outgoingTuplesPerTaskLock.lock();
	try {
	    Map<Long, Integer> toTaskMap = outgoingTuplesPerTask.get(fromTaskId);
	    if (toTaskMap == null) {
		toTaskMap = new HashMap<Long, Integer>();
		outgoingTuplesPerTask.put(fromTaskId, toTaskMap);
	    }

	    Integer count = toTaskMap.get(toTaskId);
	    if (count == null)
		count = new Integer(0);
	    count = new Integer(count.intValue() + 1);
	    toTaskMap.put(toTaskId, count);

	    // System.out.println("Task " + fromTaskId.intValue() + " emitted " + count.intValue() + " tuples to task " + outTask);

	} finally {
	    outgoingTuplesPerTaskLock.unlock();
	}
    }

    public void notifyIncomingTuple(long toTask, Tuple tuple) {

	Long fromTaskId = (long) tuple.getSourceTask();
	Long toTaskId = new Long(toTask);

	incomingTuplesPerTaskLock.lock();

	try {
	    Map<Long, Integer> toTaskMap = incomingTuplesPerTask.get(fromTaskId);
	    if (toTaskMap == null) {
		toTaskMap = new HashMap<Long, Integer>();
		incomingTuplesPerTask.put(fromTaskId, toTaskMap);
	    }

	    Integer count = toTaskMap.get(toTaskId);
	    if (count == null)
		count = new Integer(0);
	    count = new Integer(count.intValue() + 1);
	    toTaskMap.put(toTaskId, count);

	    // System.out.println("Task " + toTask + " received " + count.intValue() + " tuples from " + fromTaskId.intValue());

	} finally {
	    incomingTuplesPerTaskLock.unlock();
	}
    }
}
