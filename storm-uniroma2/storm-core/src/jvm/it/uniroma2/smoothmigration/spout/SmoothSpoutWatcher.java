package it.uniroma2.smoothmigration.spout;

import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import it.uniroma2.smoothmigration.bolt.SmoothBoltWatcher;
import it.uniroma2.smoothmigration.dsm.BackupManagement;
import it.uniroma2.smoothmigration.dsm.ZookeeperWatcher;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.Utils;
import it.uniroma2.taos.commons.dao.TrafficTuple;
import it.uniroma2.taos.commons.network.ExecutorTopo;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.network.TaskTopo;
import it.uniroma2.taos.commons.persistence.MongoManager;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Values;

public class SmoothSpoutWatcher implements Runnable {
    private boolean migrating = false;
    private boolean rdy = false;
    private boolean backup = false;
    private boolean destinationMigrating = false;
    private SmoothSpout smoothSpout;
    private ObjectMapper mapper = new ObjectMapper();
    private SmoothSpoutOutputCollector soc;
    private ArrayList<TaskTopo> taskOutputTask = new ArrayList<TaskTopo>();
    private ArrayList<TaskTopo> migratingOutputTasks = new ArrayList<TaskTopo>();
    private ArrayList<TaskTopo> eofTasks = new ArrayList<TaskTopo>();
    private Date lastupdate = new Date(0);
    private static final Long updateInterval = 1000l;
    private TaskTopo currentTask = null;
    private ZookeeperWatcher zkw;
    private BackupManagement backupManagement;
    private ArrayList<TaskTopo> smooths = new ArrayList<TaskTopo>();
    private boolean waitCreated = false;
    public final static String OUT_SEQNUM_KEY = "outseqnum";
    private long time = System.currentTimeMillis();
    private boolean resetTime = true;
    private Integer TIMEOUT;
    private ArrayList<TaskTopo> rdyLearn = new ArrayList<TaskTopo>();

    public synchronized void updateTimeout() {
	time = System.currentTimeMillis();
    }

    public SmoothSpoutWatcher(SmoothSpout sb, SmoothSpoutOutputCollector soc) {

	smoothSpout = sb;
	this.soc = soc;
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	TIMEOUT = (Integer) confs.get(Config.TAOS_SMOOTH_EOF_TIMEOUT);
	Integer zkport = (Integer) confs.get(Config.STORM_ZOOKEEPER_PORT);
	Object obj = confs.get(Config.STORM_ZOOKEEPER_SERVERS);
	List<String> servers = (List<String>) obj;
	currentTask = new TaskTopo(smoothSpout.getTopologyContext().getThisTaskId(), smoothSpout.getTopologyContext().getStormId());
	zkw = new ZookeeperWatcher(currentTask.getTopoId(), true);
	ArrayList<TaskTopo> smooths;
	PlacementResponse pr;
	synchronized (zkw.getZoo()) {
	    smooths = zkw.getSmoothTask();
	    pr = zkw.getPr();
	}
	while (pr == null) {
	    Logging.append("Need pr...must wait", Level.INFO, this.getClass());
	    pr = zkw.getPr();
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}

	try {
	    zkw.declareSmooth("/sm/smooth/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), mapper.writeValueAsBytes(currentTask));
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

	taskOutputTask = pr.getOutputTasks(smoothSpout.getTopologyContext().getStormId(), smoothSpout.getTopologyContext().getThisComponentId());
	ArrayList<TaskTopo> keep = new ArrayList<TaskTopo>();
	for (TaskTopo taskTopo : taskOutputTask) {
	    if (smooths.contains(taskTopo))
		keep.add(taskTopo);
	}
	taskOutputTask = keep;

	boolean mustExist = zkw.exists("/sm/wait/" + smoothSpout.getTopologyContext().getThisTaskId() + "@"
		+ smoothSpout.getTopologyContext().getStormId());

	if (mustExist) {
	    byte[] hostname = zkw.getData("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId());
	    backupManagement = new BackupManagement(new String(hostname), currentTask.toString(), smoothSpout.getTopologyContext(), sb);
	} else {
	    backupManagement = new BackupManagement(null, currentTask.toString(), smoothSpout.getTopologyContext(), sb);
	}

	HashMap<String, Object> state = backupManagement.restoreState(mustExist);
	if (state != null) {
	    soc.setSeqnumEmitted((HashMap<String, Integer>) state.get(OUT_SEQNUM_KEY));
	    smoothSpout.setState(state);
	}

	zkw.deleteWait("/sm/wait/" + smoothSpout.getTopologyContext().getThisTaskId() + "@" + smoothSpout.getTopologyContext().getStormId());

    }

    @Override
    public void run() {
	while (true) {
	    ArrayList<TaskTopo> migratingTasks;
	    ArrayList<TaskTopo> readyTasks;
	    ArrayList<TaskTopo> waitTask;
	    PlacementResponse pr;
	    ArrayList<TaskTopo> tmpSmooths;
	    ArrayList<String> migStreams;

	    synchronized (zkw.getZoo()) {
		migratingTasks = zkw.getMigratingTask();
		readyTasks = zkw.getReadyTask();
		waitTask = zkw.getWaitTask();
		pr = zkw.getPr();
		tmpSmooths = zkw.getSmoothTask();
		migStreams = zkw.getMigStreams(smoothSpout.getTopologyContext().getThisComponentId());

		if (migratingTasks.isEmpty() && readyTasks.isEmpty()) {
		    eofTasks.clear();
		}

		if (pr != null && (taskOutputTask == null || (new Date()).getTime() - lastupdate.getTime() > updateInterval)) {
		    for (TaskTopo taskTopo : tmpSmooths) {
			if (!smooths.contains(taskTopo))
			    smooths.add(taskTopo);
		    }

		    taskOutputTask = pr.getOutputTasks(smoothSpout.getTopologyContext().getStormId(), smoothSpout.getTopologyContext()
			    .getThisComponentId());

		    ArrayList<TaskTopo> keep = new ArrayList<TaskTopo>();
		    for (TaskTopo taskTopo : taskOutputTask) {
			if (smooths.contains(taskTopo))
			    keep.add(taskTopo);
		    }
		    taskOutputTask = keep;
		    lastupdate = new Date();
		}
	    }
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " migratingTasks " + migratingTasks.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " waittask " + waitTask.toString(), Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " rdyTasks " + readyTasks.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " taskOutputTask " + taskOutputTask.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " sent eofs " + eofTasks.toString(), Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " migrating: " + migrating, Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " destinationMigrating: " + destinationMigrating, Level.INFO,
		    this.getClass());
	    Logging.append("------------------------------------------------------------------------------------------------", Level.INFO,
		    this.getClass());

	    if (waitTask.contains(currentTask) && !waitCreated) {
		zkw.deleteWait("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId());

	    }

	    for (TaskTopo tsk : readyTasks) {
		if (!rdyLearn.contains(tsk)) {
		    updateTimeout();
		    rdyLearn.add(tsk);
		}
	    }
	    if (!migratingTasks.isEmpty()) {
		if (resetTime) {
		    updateTimeout();
		    resetTime = false;
		}
	    } else {
		rdyLearn.clear();
		resetTime = true;
	    }

	    smoothSpout.getLock().lock();

	    TaskTopo thisTask = new TaskTopo(smoothSpout.getTopologyContext().getThisTaskId(), smoothSpout.getTopologyContext().getStormId());
	    migratingOutputTasks.clear();
	    for (TaskTopo taskTopo : taskOutputTask) {
		if (migratingTasks.contains(taskTopo)) {
		    migratingOutputTasks.add(taskTopo);
		}
	    }

	    if (migratingTasks.contains(thisTask)) {
		migrating = true;
	    }

	    destinationMigrating = false;
	    ArrayList<TaskTopo> tmp = new ArrayList<TaskTopo>();
	    tmp.addAll(migratingTasks);
	    tmp.addAll(waitTask);
	    for (TaskTopo taskTopo : tmp) {
		if (taskOutputTask.contains(taskTopo)) {
		    destinationMigrating = true;
		    break;
		}
	    }

	    soc.setMigStreams(migStreams);
	    if (migrating) {
		soc.setBufferMode(false);
		String[] command = { "hostname" };
		String result = Utils.executeCommand(command);
		result = result.substring(0, result.length() - 1);
		zkw.createWait("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), result.getBytes());
		waitCreated = true;
		smoothSpout.setEnabled(false);
		if (!backup) {
		    backup = true;
		    HashMap<String, Object> toBackup = smoothSpout.getState();
		    if (toBackup == null)
			toBackup = new HashMap<String, Object>();
		    toBackup.put(OUT_SEQNUM_KEY, soc.getSeqnumEmitted());
		    if (toBackup != null)
			backupManagement.backupState(toBackup);
		}
		if (System.currentTimeMillis() - time > TIMEOUT) {
		    Logging.append("measure 1 " + TIMEOUT + " " + (System.currentTimeMillis() - time), Level.INFO, this.getClass());
		    readyTasks.addAll(migratingOutputTasks);
		}
		for (TaskTopo taskTopo : migratingTasks) {
		    if(taskTopo.isCrashed() && !readyTasks.contains(taskTopo))
		    {
			readyTasks.add(taskTopo);
		    }
		}
		if (readyTasks.containsAll(migratingOutputTasks) && !rdy && backup) {
		    Logging.append("measure 2 " + readyTasks.toString() + " " + migratingOutputTasks.toString(), Level.INFO, this.getClass());
		    rdy = true;
		    try {
			zkw.createReady("/sm/ready/" + thisTask.getTaskId() + "@" + thisTask.getTopoId(), mapper.writeValueAsBytes(thisTask));
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
		    zkw.shutdown();
		}
		for (TaskTopo taskTopo : taskOutputTask) {
		    // mando eof a tutte le destinazioni smooth
		    if (soc.getTupleBuffer().isEmpty()) {
			if (!eofTasks.contains(taskTopo)) {
			    updateTimeout();
			    eofTasks.add(taskTopo);
			}
			Logging.append(currentTask.toString() + " sendingEof " + taskTopo.toString(), Level.INFO, this.getClass());
			soc.getOc().emitDirect(taskTopo.getTaskId(), "eofs", new Values("eof"));

		    } else
			updateTimeout();

		}
	    } else if (destinationMigrating) {
		soc.setBufferMode(true);
		for (TaskTopo taskTopo : taskOutputTask) {
		    // mando eof alle destinazioni che stanno migrando
		    if (migratingTasks.contains(taskTopo)) {
			if (!eofTasks.contains(taskTopo)) {
			    eofTasks.add(taskTopo);
			    updateTimeout();
			}

			Logging.append(currentTask.toString() + " sendingEof " + taskTopo.toString(), Level.INFO, this.getClass());
			soc.getOc().emitDirect(taskTopo.getTaskId(), "eofs", new Values("eof"));

		    }
		}
	    } else {
		smoothSpout.setEnabled(true);
		soc.setBufferMode(false);
	    }

	    smoothSpout.getLock().unlock();

	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

    public ArrayList<TaskTopo> getTaskOutputTask() {
	return taskOutputTask;
    }

    public BackupManagement getBackupManagement() {
	return backupManagement;
    }

    public void setTaskOutputTask(ArrayList<TaskTopo> taskOutputTask) {
	this.taskOutputTask = taskOutputTask;
    }

}
