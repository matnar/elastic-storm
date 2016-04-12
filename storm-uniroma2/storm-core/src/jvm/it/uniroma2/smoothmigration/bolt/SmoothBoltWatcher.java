package it.uniroma2.smoothmigration.bolt;

import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;
import it.uniroma2.smoothmigration.dsm.BackupManagement;
import it.uniroma2.smoothmigration.dsm.ZookeeperWatcher;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.Utils;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.network.TaskStream;
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
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import backtype.storm.Config;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.Values;

public class SmoothBoltWatcher implements Runnable {
    byte[] currentJsonTask;
    TaskTopo currentTask;
    private ZookeeperWatcher zkw;
    private boolean migrating = false;
    private boolean waitingUpstreamEOF = false;
    private ArrayList<TaskTopo> rdyLearn = new ArrayList<TaskTopo>();
    private boolean rdy = false;
    private boolean backup = false;
    private SmoothBolt smoothBolt;
    private ObjectMapper mapper = new ObjectMapper();
    private SmoothOutputCollector soc;
    private ArrayList<TaskTopo> migratingSourceTasks = new ArrayList<TaskTopo>();
    private ArrayList<TaskTopo> taskOutputTask = new ArrayList<TaskTopo>();
    private ArrayList<TaskTopo> taskInputTasks = new ArrayList<TaskTopo>();
    private ArrayList<TaskTopo> eofTasks = new ArrayList<TaskTopo>();
    private Date lastupdate = new Date(0);
    private static final Long updateInterval = 1000l;
    private BackupManagement backupManagement;
    private final static String IN_SEQNUM_KEY = "inseqnum";
    private final static String OUT_SEQNUM_KEY = "outseqnum";
    private ArrayList<TaskTopo> smooths = new ArrayList<TaskTopo>();
    private ArrayList<TaskTopo> activeThreads = new ArrayList<TaskTopo>();
    private boolean waitCreated = false;
    private long time = System.currentTimeMillis();
    private boolean resetTime = true;
    private Integer TIMEOUT;
    private ArrayList<TaskTopo> migratingTasks = new ArrayList<TaskTopo>();

    public ArrayList<TaskTopo> getMigratingTasks() {
	return migratingTasks;
    }

    public synchronized void updateTimeout() {
	time = System.currentTimeMillis();
    }

    public SmoothBoltWatcher(SmoothBolt sb, SmoothOutputCollector soc) {

	smoothBolt = sb;
	this.soc = soc;
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	TIMEOUT = (Integer) confs.get(Config.TAOS_SMOOTH_EOF_TIMEOUT);
	Integer zkport = (Integer) confs.get(Config.STORM_ZOOKEEPER_PORT);
	Object obj = confs.get(Config.STORM_ZOOKEEPER_SERVERS);
	List<String> servers = (List<String>) obj;
	currentTask = new TaskTopo(smoothBolt.getTopologyContext().getThisTaskId(), smoothBolt.getTopologyContext().getStormId());
	zkw = new ZookeeperWatcher(currentTask.getTopoId(), true);
	PlacementResponse pr;
	ArrayList<TaskTopo> smooths;
	synchronized (zkw.getZoo()) {
	    pr = zkw.getPr();
	    smooths = zkw.getSmoothTask();
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
	    currentJsonTask = mapper.writeValueAsBytes(currentTask);
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

	/*
	 * Node announces smooth status.
	 */

	try {
	    zkw.declareSmooth("/sm/smooth/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), mapper.writeValueAsBytes(currentTask));
	} catch (JsonGenerationException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	} catch (JsonMappingException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	} catch (IOException e1) {
	    // TODO Auto-generated catch block
	    e1.printStackTrace();
	}

	//check if there exist a status to download
	boolean mustExist = zkw.exists("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId());
	if (mustExist) {
	    byte[] hostname = zkw.getData("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId());
	    backupManagement = new BackupManagement(new String(hostname), currentTask.toString(), smoothBolt.getTopologyContext(), sb);
	    String result = zkw.getHostname();
	    if(sb.getTopologyContext().getThisComponentId().equals("detector") && !result.equals(new String(hostname)))
	    {
		zkw.getManager().putMovDetectorsTmp();
	    }

	} else {
	    backupManagement = new BackupManagement(null, currentTask.toString(), smoothBolt.getTopologyContext(), sb);
	}

	//restores state if there exists a status to download
	HashMap<String, Object> toSet = backupManagement.restoreState(mustExist);
	if (toSet != null && !toSet.keySet().isEmpty()) {
	    smoothBolt.setState(toSet);
	    soc.setSeqnumEmitted((HashMap<String, Integer>) toSet.get(OUT_SEQNUM_KEY));
	    smoothBolt.setInSeqNum((HashMap<String, Integer>) toSet.get(IN_SEQNUM_KEY));
	}

	smoothBolt.getInBuffer().addAll(backupManagement.restoreTuples(mustExist));

	/*
	 * At start remove wait node from zookeeper for this task if one does exist. This will tell other tasks that this task is ready to elaborate workload
	 */

	zkw.deleteWait("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId());

	taskInputTasks = pr.getInputTasks(currentTask.getTaskId(), currentTask.getTopoId());
	taskOutputTask = pr.getOutputTasks(smoothBolt.getTopologyContext().getStormId(), smoothBolt.getTopologyContext().getThisComponentId());

	ArrayList<TaskTopo> keep = new ArrayList<TaskTopo>();
	for (TaskTopo taskTopo : taskInputTasks) {
	    if (smooths.contains(taskTopo))
		keep.add(taskTopo);
	}
	taskInputTasks = keep;
	keep = new ArrayList<TaskTopo>();
	for (TaskTopo taskTopo : taskOutputTask) {
	    if (smooths.contains(taskTopo))
		keep.add(taskTopo);
	}
	taskOutputTask = keep;

	lastupdate = new Date();

    }

    @Override
    public void run() {
	while (true) {
	    // Collect all information on cluster state
	    ArrayList<TaskTopo> readyTasks;
	    ArrayList<TaskTopo> waitTask;
	    ArrayList<TaskTopo> rcvdEofs;
	    ArrayList<String> migStreams;
	    ArrayList<TaskTopo> tmpSmooth;
	    PlacementResponse pr;
	    synchronized (zkw.getZoo()) {
		migratingTasks = zkw.getMigratingTask();
		readyTasks = zkw.getReadyTask();
		waitTask = zkw.getWaitTask();
		rcvdEofs = smoothBolt.getEOFS();
		migStreams = zkw.getMigStreams(smoothBolt.getTopologyContext().getThisComponentId());
		tmpSmooth = zkw.getSmoothTask();
		pr = zkw.getPr();

		if (pr != null
			&& (taskOutputTask == null || taskInputTasks == null || (new Date()).getTime() - lastupdate.getTime() > updateInterval)) {
		    taskInputTasks = pr.getInputTasks(currentTask.getTaskId(), currentTask.getTopoId());
		    taskOutputTask = pr.getOutputTasks(smoothBolt.getTopologyContext().getStormId(), smoothBolt.getTopologyContext()
			    .getThisComponentId());

		    for (TaskTopo taskTopo : tmpSmooth) {
			if (!smooths.contains(taskTopo))
			    smooths.add(taskTopo);
		    }

		    ArrayList<TaskTopo> keep = new ArrayList<TaskTopo>();
		    for (TaskTopo taskTopo : taskInputTasks) {
			if (smooths.contains(taskTopo))
			    keep.add(taskTopo);
		    }
		    taskInputTasks = keep;
		    keep = new ArrayList<TaskTopo>();
		    for (TaskTopo taskTopo : taskOutputTask) {
			if (smooths.contains(taskTopo))
			    keep.add(taskTopo);
		    }
		    taskOutputTask = keep;
		    lastupdate = new Date();
		}
	    }

	    ArrayList<TaskTopo> notReadyTasks = new ArrayList<TaskTopo>();
	    notReadyTasks.addAll(migratingTasks);
	    notReadyTasks.removeAll(readyTasks);
	    ArrayList<TaskTopo> missingEofs = new ArrayList<TaskTopo>();
	    missingEofs.addAll(taskInputTasks);
	    missingEofs.removeAll(rcvdEofs);

	    for (TaskTopo tsk : readyTasks) {
		if(!rdyLearn.contains(tsk))
		{
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

	    //print out view of cluster state from this task point of view
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " migratingTasks " + migratingTasks.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " migStreams " + migStreams.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " waittask " + waitTask.toString(), Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " rdytask " + readyTasks.toString(), Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " taskOutputTask list is " + taskOutputTask.toString(),
		    Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " taskInputTasks list is " + taskInputTasks.toString(),
		    Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " isBufferMode  " + soc.isBufferMode(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " isSkip  " + smoothBolt.isSkip(), Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " received eof  " + rcvdEofs.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " missingEofs eof  " + missingEofs.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " notReadyTasks  " + notReadyTasks.toString(), Level.INFO,
		    this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " inputBufffer size  " + smoothBolt.getInBuffer().size(),
		    Level.INFO, this.getClass());
	    Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " outputBufffer size  " + soc.getTupleBuffer().size(),
		    Level.INFO, this.getClass());

	    synchronized (eofTasks) {
		Logging.append(currentTask.getTaskId() + "@" + currentTask.getTopoId() + " sent eof  " + eofTasks.toString(), Level.INFO,
			this.getClass());
		if ((!eofTasks.isEmpty() || !smoothBolt.getEOFS().isEmpty()) && readyTasks.isEmpty() && migratingTasks.isEmpty()) {
		    eofTasks.clear();
		    // non ci sono migrazioni in corso, cancello gli eof
		    smoothBolt.clearEOFS();
		    rcvdEofs.clear();
		}
	    }

	    Logging.append(" ------------------------------------------------------------------------------------------------ ", Level.INFO,
		    this.getClass());

	    Logging.append(currentTask.toString() + " waitCreated: " + waitCreated, Level.INFO, this.getClass());
	    if (waitTask.contains(currentTask) && !waitCreated) {
		Logging.append(" waitDetected deleting " + "/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), Level.INFO,
			this.getClass());
		zkw.deleteWait("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId());
	    }

	    /*
	     * If this task is in the list of migrating tasks published by nimbus. Enter migrating state.
	     */
	    if (migratingTasks.contains(currentTask)) {
		migrating = true;
		Logging.append(" migrating: " + migrating, Level.INFO, this.getClass());

	    } else {
		/*
		 * Otherwise check if some source is migrating
		 */
		migratingSourceTasks.clear();
		for (TaskTopo InputTask : taskInputTasks) {
		    if (migratingTasks.contains(InputTask))
			migratingSourceTasks.add(InputTask);
		}
		/*
		 * if some upstream task is migrating enter source migrating state
		 */
		if (!migratingSourceTasks.isEmpty()) {
		    waitingUpstreamEOF = true;
		    Logging.append("waitingUpstreamEOF: " + waitingUpstreamEOF, Level.INFO, this.getClass());
		}

	    }

	    /*
	     * check if some destination is migrating, in this case enter destination migrating state, this state is ignored if this task is in migrating state
	     */
	    ArrayList<TaskTopo> tmp = new ArrayList<TaskTopo>();
	    tmp.addAll(migratingTasks);
	    tmp.addAll(waitTask);
	    boolean destinationMigrating = false;
	    for (TaskTopo taskTopo : tmp) {
		if (taskOutputTask.contains(taskTopo)) {
		    destinationMigrating = true;
		}
	    }

	    if (migrating) {

		smoothBolt.getLock().lock();
		Logging.append("In Migrating block " + currentTask.getTaskId(), Level.INFO, this.getClass());

		String[] command = { "hostname" };
		String result = Utils.executeCommand(command);
		result = result.substring(0, result.length() - 1);
		zkw.createWait("/sm/wait/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), result.getBytes());
		waitCreated = true;
		smoothBolt.setSkip(true);
		ArrayList<TaskTopo> selfLoopFix = new ArrayList<TaskTopo>();
		selfLoopFix.addAll(taskOutputTask);
		selfLoopFix.removeAll(taskInputTasks);
		//stop waiting for eofs after TIMEOUT
		if (System.currentTimeMillis() - time > TIMEOUT) {
		    rcvdEofs.clear();
		    rcvdEofs.addAll(taskInputTasks);
		    readyTasks.clear();
		    readyTasks.addAll(selfLoopFix);
		}
		//ignore tasks which are marked as crashed by the fetcher
		for (TaskTopo taskTopo : migratingTasks) {
		    if(!rcvdEofs.contains(taskTopo) && taskTopo.isCrashed())
		    {
			rcvdEofs.add(taskTopo);
		    }
		    if(!readyTasks.contains(taskTopo) && taskTopo.isCrashed())
		    {
			readyTasks.add(taskTopo);
		    }
		}
		//if all eofs are received from input tasks and outbuffer is empty proceed to state backup.
		if (rcvdEofs.containsAll(taskInputTasks) && !backup && soc.getTupleBuffer().isEmpty()) {
		    System.out.println(currentTask.getTaskId() + " start backup ");
		    Date time = new Date();
		    long size = smoothBolt.getInBuffer().size();
		    zkw.getManager().putTempTupleCount(size, currentTask.getTaskId() + "@" + currentTask.getTopoId());
		    backupManagement.backupTuples(smoothBolt.getInBuffer());
		    System.out.println("backup tuple time " + ((new Date()).getTime() - time.getTime()));
		    HashMap<String, Object> toBackup = smoothBolt.getState();
		    if (toBackup == null)
			toBackup = new HashMap<String, Object>();
		    toBackup.put(OUT_SEQNUM_KEY, soc.getSeqnumEmitted());
		    toBackup.put(IN_SEQNUM_KEY, smoothBolt.getInSeqNum());
		    Logging.append("waitingUpstreamEOF: " + waitingUpstreamEOF, Level.INFO, this.getClass());
		    time = new Date();
		    backupManagement.backupState(toBackup);
		    System.out.println(currentTask.getTaskId() + " end backup " + ((new Date()).getTime() - time.getTime()));
		    /* pulisco gli EOFS perche` li ho usati */
		    smoothBolt.clearEOFS();
		    backup = true;

		}
		//if backup was performerd and all destination are ready declare this task as ready to migrate
		if (!rdy && readyTasks.containsAll(selfLoopFix) && backup) {
		    rdy = true;
		    zkw.createReady("/sm/ready/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), currentJsonTask);
		    zkw.shutdown();
		}
		smoothBolt.getLock().unlock();
		
	    } 
	    //if in source migrating state.
	    else if (waitingUpstreamEOF) {
		if (System.currentTimeMillis() - time > TIMEOUT) {
		    rcvdEofs.clear();
		    rcvdEofs.addAll(migratingSourceTasks);
		}

		smoothBolt.getLock().lock();
		Logging.append("In waitingUpstreamEOF block " + currentTask.getTaskId(), Level.INFO, this.getClass());
		// set this task ready as soon as all eofs are received or timed out. resume computation
		if (rcvdEofs.containsAll(migratingSourceTasks)) {
		    zkw.createReady("/sm/ready/" + currentTask.getTaskId() + "@" + currentTask.getTopoId(), currentJsonTask);
		    /* pulisco gli EOFS perche` li ho usati */
		    smoothBolt.clearEOFS();
		    /* riprendo la computazione */
		    smoothBolt.setSkip(false);
		    waitingUpstreamEOF = false;

		} 
		//otherwise stop computation and buffer input
		else {
		    smoothBolt.setSkip(true);
		}
		smoothBolt.getLock().unlock();
	    } else {
		smoothBolt.setSkip(false);
	    }

	    /* begin output management */

	    /*
	     * if migrating send eofs to all destionation tasks. computation is sospended and out buffer is unloaded
	     */

	    if (migrating) {
		smoothBolt.getLock().lock();
		Logging.append("In migrating (output) block " + currentTask.getTaskId(), Level.INFO, this.getClass());
		soc.setBufferMode(false);
		soc.setMigStreams(new ArrayList<String>());
		
		for (TaskTopo taskTopo : taskOutputTask) {

		    if (smooths.contains(taskTopo)) {

			synchronized (activeThreads) {
			    if (!activeThreads.contains(taskTopo))
				new Thread(new EofThread(taskTopo)).start();
			}

		    }
		}
		smoothBolt.getLock().unlock();
	    } 
	    //if destination is migrating, simply send eofs to migrating destinations, start buffering tuples for those tasks
	    else if (destinationMigrating) {
		smoothBolt.getLock().lock();
		Logging.append("In destinationMigrating block " + currentTask.getTaskId(), Level.INFO, this.getClass());
		ArrayList<String> streams = new ArrayList<String>();
		streams.addAll(migStreams);
		soc.setMigStreams(streams);
		soc.setBufferMode(true);
		for (TaskTopo taskTopo : taskOutputTask) {
		    if (migratingTasks.contains(taskTopo)) {
			synchronized (activeThreads) {
			    if (!activeThreads.contains(taskTopo))
				new Thread(new EofThread(taskTopo)).start();
			}

		    }
		}
		smoothBolt.getLock().unlock();
	    } 
	    //otherwise unlock output and resume normal transmission
	    else {
		soc.setBufferMode(false);
		soc.setMigStreams(new ArrayList<String>());
		
	    }

	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}
    }

    class EofThread implements Runnable {
	TaskTopo toEof;
	boolean imnew = false;

	public EofThread(TaskTopo t) {
	    toEof = t;
	    synchronized (eofTasks) {
		if (!eofTasks.contains(toEof)) {
		    imnew = true;
		    eofTasks.add(toEof);
		}
	    }

	}

	@Override
	public void run() {

	    Logging.append(currentTask.toString() + " sendingEof " + toEof.toString(), Level.INFO, this.getClass());

	    synchronized (activeThreads) {
		activeThreads.add(toEof);
	    }
	    while (!soc.getTupleBuffer().isEmpty() && migrating) {
		updateTimeout();
		try {
		    Thread.sleep(1000);
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }
	    soc.getOc().emitDirect(toEof.getTaskId(), "eofs", new Values("eof"));
	    if (imnew) {
		updateTimeout();
	    }

	    synchronized (activeThreads) {
		activeThreads.remove(toEof);
	    }

	}

    }

    public ArrayList<TaskTopo> getTaskInputTasks() {
	return taskInputTasks;
    }

    public void setTaskInputTasks(ArrayList<TaskTopo> taskInputTasks) {
	this.taskInputTasks = taskInputTasks;
    }

    public ArrayList<TaskTopo> getMigratingSourceTasks() {
	return migratingSourceTasks;
    }

    public void setMigratingSourceTasks(ArrayList<TaskTopo> migratingSourceTasks) {
	this.migratingSourceTasks = migratingSourceTasks;
    }

    public ArrayList<TaskTopo> getTaskOutputTask() {
	return taskOutputTask;
    }

    public void setTaskOutputTask(ArrayList<TaskTopo> taskOutputTask) {
	this.taskOutputTask = taskOutputTask;
    }

    public BackupManagement getBackupManagement() {
	return backupManagement;
    }
    public ZookeeperWatcher getZkw() {
        return zkw;
    }


}
