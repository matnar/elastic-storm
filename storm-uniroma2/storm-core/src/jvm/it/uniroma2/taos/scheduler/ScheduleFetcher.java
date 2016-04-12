package it.uniroma2.taos.scheduler;

import it.uniroma2.adaptivescheduler.scheduler.BootstrapScheduler;
import it.uniroma2.smoothmigration.dsm.ZookeeperWatcher;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.dao.ExecutorTuple;
import it.uniroma2.taos.commons.dao.NodeTuple;
import it.uniroma2.taos.commons.dao.TrafficTuple;
import it.uniroma2.taos.commons.network.Assignment;
import it.uniroma2.taos.commons.network.ExecutorTopo;
import it.uniroma2.taos.commons.network.NodeSlots;
import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.network.TaskTopo;
import it.uniroma2.taos.commons.persistence.MongoManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;

import javax.print.attribute.HashAttributeSet;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import backtype.storm.Config;
import backtype.storm.generated.Grouping;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.GeneralTopologyContext;

public class ScheduleFetcher extends BootstrapScheduler {

    private static PlacementResponse lastResp;
    private static MongoManager manager;
    private static long SCHEDULE_FETCH_TIME;
    private static String URL;
    private ObjectMapper mapper = new ObjectMapper();
    private ZookeeperWatcher zkw = new ZookeeperWatcher(null, false);
    private ArrayList<TaskTopo> learnedSmooth = new ArrayList<TaskTopo>();
    private static double UTIL_NORM;
    private static String SPOUT_NAME;
    private double UPPER_SCALING_FACTOR;
    private double LOWER_SCALING_FACTOR;
    private double WEAK_SCALE_THR;
    private Integer MIGRATION_TIMEOUT;
    private double SCALE_TO_MAX_THR;
    private boolean SMOOTH_ENABLED = true;
    private static HashMap<String, String> rebalanceCommandsByTopo = new HashMap<String, String>();
    private static String stormLoc = "";
    private static int MAX_DOWNSCALE_FACTOR = 0;
    private boolean rebalancing = false;
    private ArrayList<NodeSlots> NodesInLastAssignment = new ArrayList<NodeSlots>();
    private static ArrayList<Integer> migratingTasks = new ArrayList<Integer>();
    private static Date lastassignment = new Date();
    

    public boolean checkNumNodes(ArrayList<NodeSlots> toCheck, ArrayList<TaskTopo> Crashed) {
	for (NodeSlots ns : NodesInLastAssignment) {
	    if (!toCheck.contains(ns)) {
		for (Assignment as : lastResp.getAssignment()) {
		    if (as.getNodeId().equals(ns.getNodeId())) {
			for (String exeid : as.getExecutorsIds()) {
			    ArrayList<Integer> tasks = lastResp.getTasksByExecutorId(exeid);
			    for (Integer i : tasks) {
				TaskTopo toAdd = new TaskTopo(i, as.getTopoId());
				toAdd.setCrashed(true);
				if (!Crashed.contains(toAdd))
				    Crashed.add(toAdd);
			    }
			}
		    }
		}
	    }
	}
	if (NodesInLastAssignment.isEmpty() || toCheck.isEmpty())
	    return false;
	if (NodesInLastAssignment.containsAll(toCheck) && NodesInLastAssignment.size() == toCheck.size())
	    return true;
	return false;
    }

    // private static ArrayList<Integer> migratingTasks = new ArrayList<Integer>();

    public ScheduleFetcher() {
	super();
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	String mongoIp = (String) confs.get(Config.TAOS_MONGO_DB_IP);
	Integer mongoPort = (Integer) confs.get(Config.TAOS_MONGO_DB_PORT);
	int ttl = (Integer) confs.get(Config.TAOS_DB_TTL);
	Double updateDamping = (Double) confs.get(Config.TAOS_METRICS_SMOOTHING);
	SCHEDULE_FETCH_TIME = ((Integer) confs.get(Config.TAOS_SCHEDULE_FETCH_TIME)).longValue();
	manager = new MongoManager(mongoIp, mongoPort, ttl, updateDamping.floatValue());
	LOWER_SCALING_FACTOR = (Double) confs.get(Config.AUTOSCALING_DOWNSCALE_THRESHOLD);
	UPPER_SCALING_FACTOR = (Double) confs.get(Config.AUTOSCALING_UPSCALE_THRESHOLD);
	UTIL_NORM=(Double) confs.get(Config.METRICS_UTIL_NORM);
	SPOUT_NAME=(String) confs.get(Config.METRICS_SPOUT_NAME);
	WEAK_SCALE_THR = (Double) confs.get(Config.AUTOSCALING_WEAK_SCALE_THRESHOLD);
	SCALE_TO_MAX_THR = (Double) confs.get(Config.AUTOSCALING_SCALE_TO_MAX_THRESHOLD);
	SMOOTH_ENABLED = (Boolean) confs.get(Config.TAOS_SMOOTH_ENABLED);
	MIGRATION_TIMEOUT = (Integer) confs.get(Config.TAOS_SMOOTH_TIMEOUT);
	int port = (Integer) confs.get(Config.TAOS_GENERATOR_PORT);
	String ip = (String) confs.get(Config.TAOS_GENERATOR_IP);
	MAX_DOWNSCALE_FACTOR = (Integer) confs.get(Config.AUTOSCALING_MAX_DOWNSCALE_FACTOR);
	URL = "http://" + ip + ":" + port + "/ScheduleGeneratorServlet/placement";
	zkw.createDirStructure();
	zkw.setEnableLog(false);
	try {
	    String path = this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
	    stormLoc = path.substring(0, path.lastIndexOf("/"));
	    stormLoc = stormLoc.substring(0, stormLoc.lastIndexOf("/"));
	    stormLoc += "/bin";
	} catch (URISyntaxException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	manager.delLat();
	manager.delTempMigSize();
	manager.delTotalMigSize();
	manager.delNe();
	manager.delMigTime();
	manager.delNetwork();
	manager.delTupleCount();
	manager.delTempTupleCount();
	manager.delUtilization();
	manager.delMDetectors();
	manager.delMovDetectors();
	manager.delMovDetectorsTmp();
	Thread t = new Thread(new Monitoring());
	t.start();

    }

    @Override
    public void prepare(Map conf) {
	// TODO Auto-generated method stub

    }
    
    class Monitoring implements Runnable {
	public Date start = null;
	public Date lastExecutorUpdate = new Date();
	public long UPDATE_INTERVAL = 5000;
	@Override
	public void run() {
	    while (true) {
		if (SMOOTH_ENABLED) {
		    if (!zkw.getWaitTask().isEmpty() && start == null) {
			start = new Date();
		    } else if (start != null && zkw.getWaitTask().isEmpty()) {
			try {
			    Thread.sleep(500);
			} catch (InterruptedException e) {
			    // TODO Auto-generated catch block
			    e.printStackTrace();
			}
			if (zkw.getWaitTask().isEmpty()) {
			    manager.putMigTime((new Date()).getTime() - start.getTime());
			    start = null;
			    long total = manager.getTempMigSize();
			    manager.putTotalMigSize(total);
			    manager.delTempMigSize();
			    total = manager.getTempTupleCount();
			    manager.putTupleCount(total);
			    manager.delTempTupleCount();
			    int toPut = manager.getMovDetectorsCount();
			    manager.putMovDetectors(toPut);
			    manager.delMovDetectorsTmp();
			}

		    }
		} else {
		    if(!migratingTasks.isEmpty() && start == null)
		    {
			start=new Date();
		    }
		    else if(start!=null)
		    {
			ArrayList<Integer> rdy=manager.getRdyTasks();
			if(rdy.containsAll(migratingTasks))
			{
			    manager.putMigTime((new Date()).getTime() - start.getTime());
			    start=null;
			    manager.delRdyTasks();
			    migratingTasks=new ArrayList<Integer>();
			}
		    }
		}
		if (new Date().getTime() - lastExecutorUpdate.getTime() > UPDATE_INTERVAL) {
		    if (lastResp != null) {
			int executors = 0;
			for (Assignment ass : lastResp.getAssignment()) {
			    executors += ass.getExecutorsIds().size();
			}
			manager.putNe(executors);

			double min = Double.MAX_VALUE;
			double max = 0;
			double avg = 0;
			ArrayList<NodeTuple> list = manager.getNode();
			for (NodeTuple nt : list) {
			    double div = (nt.getCoreMhz() * nt.getCoreCount()) * UTIL_NORM;
			    min = Math.min((nt.getCurrentNodeMhz() / div), min);
			    max = Math.max((nt.getCurrentNodeMhz() / div), max);
			    avg += (nt.getCurrentNodeMhz() / div);
			}
			avg = avg / list.size();
			avg = Math.min(avg, 1);
			max = Math.min(max, 1);
			min = Math.min(min, 1);

			manager.putUtilization(min, max, avg);

			for (String topo : lastResp.getTopos()) {
			    manager.putLatency(getLatency(topo, SPOUT_NAME, UPDATE_INTERVAL / 1000));
			}
			lastExecutorUpdate = new Date();
		    }

		}
		try {
		    Thread.sleep(1000);
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }

	}

	public long getLatency(String topology, String compName, long window) {
	    try {
		String url = "http://localhost:8080/api/v1/topology/" + topology + "/component/" + compName + "?window=" + window;
		Logging.append("url " + url, Level.INFO, ScheduleFetcher.class);

		HttpClient httpClient = HttpClientBuilder.create().build();
		HttpGet getRequest = new HttpGet(url);
		HttpResponse response = httpClient.execute(getRequest);
		if (response.getStatusLine().getStatusCode() != 200) {
		    Logging.append("Failed : HTTP error code : " + response.getStatusLine().getStatusCode(), Level.INFO, ScheduleFetcher.class);
		}

		BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

		String output;
		String content = "";

		while ((output = br.readLine()) != null) {
		    content += output;
		}
		JsonParser parser = new JsonParser();
		JsonObject o = (JsonObject) parser.parse(content);
		JsonArray ja = o.getAsJsonArray("spoutSummary");
		if (ja == null || ja.get(0) == null)
		    return 0;
		o = ja.get(0).getAsJsonObject();
		String value = o.get("completeLatency").getAsString();
		Logging.append("Metric resp is " + value, Level.INFO, ScheduleFetcher.class);
		String format;
		if (value.contains(".")) {
		    format = value.split("\\.")[0];
		} else
		    format = value;
		httpClient.getConnectionManager().shutdown();

		return Long.parseLong(format);

	    } catch (MalformedURLException e) {
		Logging.append(e.getMessage(), Level.INFO, ScheduleFetcher.class);
		e.printStackTrace();

	    } catch (IOException e) {
		Logging.append(e.getMessage(), Level.INFO, ScheduleFetcher.class);
		e.printStackTrace();

	    }
	    return 0;

	}

    }

    @Override
    public void scheduleUsingContext(Topologies topologies, Cluster cluster, Map<String, GeneralTopologyContext> topologiesContext) {
	Logging.append("Starting taos scheduler ------------------------------------------------------------------------------", Level.INFO,
		ScheduleFetcher.class);
	try {
	    Logging.append(this.getClass().getProtectionDomain().getCodeSource().getLocation().toURI().getPath(), Level.INFO, ScheduleFetcher.class);
	} catch (URISyntaxException e1) {
	    e1.printStackTrace();
	}
	if (!zkw.getWaitTask().isEmpty()) {
	    lastassignment = new Date();
	    return;
	}

	ArrayList<TaskTopo> ackers=new ArrayList<TaskTopo>();
	for (TopologyDetails td : topologies.getTopologies()) {
	    Map<ExecutorDetails, String> etp = td.getExecutorToComponent();
	    for (ExecutorDetails ed : etp.keySet()) {
		if(etp.get(ed).contains("acker"))
		{
		    ackers.add(new TaskTopo(ed.getStartTask(),td.getId()));
		}
		   
	    }
	}
	Logging.append("ackers are " + ackers.toString(), Level.INFO, ScheduleFetcher.class);
	//execute, if available, a rebalance command. Skip if at least one topology is still rebalancing (its executors are to be assigned)
	if (!rebalanceCommandsByTopo.entrySet().isEmpty() && !rebalancing) {
	    String topoId = rebalanceCommandsByTopo.entrySet().iterator().next().getKey();
	    String command = rebalanceCommandsByTopo.get(topoId);
	    rebalanceCommandsByTopo.remove(topoId);
	    Process p = null;
	    try {
		p = Runtime.getRuntime().exec(stormLoc + command);
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	    rebalancing = true;
	    lastassignment = new Date();
	    Logging.append("RUNNING " + command, Level.INFO, ScheduleFetcher.class);
	}

	for (TaskTopo taskTopo : zkw.getSmoothTask()) {
	    if (!learnedSmooth.contains(taskTopo))
		learnedSmooth.add(taskTopo);
	}

	if (lastResp == null && cluster.getUsedSlots().size() != 0) {
	    lastResp = zkw.getPr();
	}

	// build list of topologies for which rebalance command was requested
	ArrayList<String> rebalanced = new ArrayList<String>();
	if (lastResp != null) {
	    for (TopologyDetails td : topologies.getTopologies()) {
		if (cluster.getUnassignedExecutors(td).size() != 0 && !lastResp.getByTopology(td.getId()).isEmpty()) {
		    rebalanced.add(td.getId());
		}
	    }
	}

	// build placement request for generator
	HashMap<String, ArrayList<Integer>> usedSlots = new HashMap<String, ArrayList<Integer>>();
	PlacementRequest pr = new PlacementRequest();
	pr.setReabalanceTopoIds(rebalanced);
	for (SupervisorDetails sDetails : cluster.getSupervisors().values()) {
	    pr.getAvailableNodes().add(new NodeSlots(sDetails.getId(), sDetails.getAllPorts().size()));
	}
	for (TopologyDetails td : topologies.getTopologies()) {
	    String tid = td.getId();
	    for (ExecutorDetails ed : td.getExecutors()) {
		String completeId = "[" + ed.getStartTask() + " " + ed.getEndTask() + "]";
		pr.getExecutors().add(new ExecutorTopo(tid, completeId));
	    }

	}
	Logging.append("Nodes are " + pr.getAvailableNodes().toString(), Level.INFO, ScheduleFetcher.class);
	Logging.append("Executors are " + pr.getExecutors().toString(), Level.INFO, ScheduleFetcher.class);
	if (SMOOTH_ENABLED)
	    Logging.append("Smooths are " + learnedSmooth.toString(), Level.INFO, ScheduleFetcher.class);
	String toSend = "";
	try {
	    toSend = mapper.writeValueAsString(pr);
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

	/* Submit request to generator to obtain new placement */
	PlacementResponse prep = postRequest(toSend, URL);
	Date now = new Date();

	if (prep == null) {
	    Logging.append("Generator is not ready returning", Level.INFO, ScheduleFetcher.class);
	    return;
	} else {
	    Logging.append("Response is " + prep.toString(), Level.INFO, ScheduleFetcher.class);
	}

	/*
	 * Check in the new placement if some worker processes are not changed, in this case copy their port assignment in the generator response.
	 * This way the same port will be reassigned to them in the assignment phase.
	 */
	if (lastResp != null) {

	    for (Assignment p : prep.getAssignment()) {
		Assignment oldplac = lastResp.getPlacementByPlacement(p);
		if (oldplac != null) {
		    p.setPort(oldplac.getPort());
		    p.setExecutorsIds(oldplac.getExecutorsIds());
		    ArrayList<Integer> slots = usedSlots.get(p.getNodeId());
		    if (slots == null) {
			slots = new ArrayList<Integer>();
			slots.add(oldplac.getPort());
			usedSlots.put(p.getNodeId(), slots);
		    } else {
			slots.add(oldplac.getPort());
		    }
		    continue;
		}
	    }

	    ArrayList<TaskTopo> crashed = new ArrayList<TaskTopo>();
	    /* Iterate all topologies, build and add their structure to the generator response */
	    for (TopologyDetails td : topologies.getTopologies()) {

		// topology---->operator----->declared streams----->destination tasks
		HashMap<String, HashMap<String, HashMap<String, ArrayList<Integer>>>> topos = prep.getStreams();
		GeneralTopologyContext gtc = topologiesContext.get(td.getId());
		HashMap<String, HashMap<String, ArrayList<Integer>>> compToStreamToTask = new HashMap<String, HashMap<String, ArrayList<Integer>>>();
		for (String cId : gtc.getComponentIds()) {
		    prep.getComponentToTask().put(cId, gtc.getComponentTasks(cId));
		    HashMap<String, ArrayList<Integer>> streamToTasks = new HashMap<String, ArrayList<Integer>>();
		    Map<String, Map<String, Grouping>> targs = gtc.getTargets(cId);
		    for (String stream : targs.keySet()) {
			ArrayList<Integer> tasks = new ArrayList<Integer>();
			Map<String, Grouping> destComps = targs.get(stream);
			for (String comp : destComps.keySet()) {
			    List<Integer> partTasks = gtc.getComponentTasks(comp);
			    tasks.addAll(partTasks);
			}
			streamToTasks.put(stream, tasks);
		    }
		    compToStreamToTask.put(cId, streamToTasks);
		}
		topos.put(td.getId(), compToStreamToTask);

		if (!checkNumNodes(pr.getAvailableNodes(), crashed)) {
		    continue;
		}

		/*
		 * If this topology is already completely deployed (all executors are assigned), ignore new deployment if according
		 * to SCHEDULE_FETCH_TIME it is too early to schedule a new change in the cluster. 
		 * If this topology is already completely deployed (all executors are assigned), ignore new deployment 
		 * if generator reports it is round robin, worst or simply not better enough than the current one. 
		 * If topology as some non assigned executors, the use this deployment in any case
		 */

		if (!(cluster.getUnassignedExecutors(td).size() > 0)
			&& (now.getTime() - lastassignment.getTime() < SCHEDULE_FETCH_TIME || prep.isIgnoreIfDeployed())) {
		    Logging.append(
			    "Redundant assignment found. Already Deployed: " + !(cluster.getUnassignedExecutors(td).size() > 0) + " too close: "
				    + (now.getTime() - lastassignment.getTime() < SCHEDULE_FETCH_TIME) + " Ignore if deployed: " + prep.isIgnoreIfDeployed(),
			    Level.INFO, ScheduleFetcher.class);
		    /* Remove new ignored assignment */
		    prep.RemoveByTopology(td.getId());
		    /* get old assignment for this topology. */
		    ArrayList<Assignment> pls = lastResp.getByTopology(td.getId());
		    for (Assignment placement : pls) {
			/*
			 * Old worker slots are used
			 */
			ArrayList<Integer> slots = usedSlots.get(placement.getNodeId());
			if (slots == null) {
			    slots = new ArrayList<Integer>();
			    slots.add(placement.getPort());
			    /*
			     * Slot is marked as already used
			     */
			    usedSlots.put(placement.getNodeId(), slots);
			} else {
			    slots.add(placement.getPort());
			}
		    }
		    /*
		     * replace new assignment with old one.
		     */
		    prep.getAssignment().addAll(pls);
		}
	    }
	    Logging.append("usedSlots are " + usedSlots.toString(), Level.INFO, ScheduleFetcher.class);

	    ArrayList<TaskTopo> allMigratingTasks = new ArrayList<TaskTopo>();

	    /* Build lisk of tasks that need to be migrated. These tasks are on worker process for which assignment has changed. 
	     * such worker process are not contained in the usedSlots list.
	     * */
	    for (SupervisorDetails sd : cluster.getSupervisors().values()) {
		/* retrieve list of currently used slots for this supervisor */
		ArrayList<Integer> slt = usedSlots.get(sd.getId());
		if (slt == null) {
		    slt = new ArrayList<Integer>();
		}
		/* itarate all slots */
		for (Integer free : sd.getAllPorts()) {
		    /* if worker process in the slot is not unchanged */
		    if (!slt.contains(free)) {
			/* retrieve list of tasks on slots to be freed, add all tasks to the list of migrating tasks */
			Assignment p = lastResp.getAssignmentByPortAndNode(free, sd.getId());
			if (p == null)
			    continue;
			for (String exe : p.getExecutorsIds()) {
			    for (Integer task : lastResp.getTasksByExecutorId(exe)) {
				if (topologies.getById(p.getTopoId()) != null)
				    allMigratingTasks.add(new TaskTopo(task, p.getTopoId()));
			    }
			}
		    }
		}
	    }

	    if (!pr.getExecutors().isEmpty() && !allMigratingTasks.isEmpty()) {
		Logging.append("crashed are " + crashed.toString(), Level.INFO, ScheduleFetcher.class);
		Logging.append("all migrating tasks are " + allMigratingTasks.toString(), Level.INFO, ScheduleFetcher.class);
		ArrayList<TaskTopo> migratingSmoothTasks = new ArrayList<TaskTopo>();


		/*
		 * if  smooth migration is enabled, put on zookeeper the list of tasks that are to be migrated and are also declared smooth.
		 */
		if (SMOOTH_ENABLED) {
		    ArrayList<TaskTopo> allMigratingTasksPlusCrashed = new ArrayList<TaskTopo>();
		    allMigratingTasksPlusCrashed.addAll(allMigratingTasks);
		    allMigratingTasksPlusCrashed.addAll(crashed);
		    zkw.postMigratingTaskList(allMigratingTasksPlusCrashed);
		    for (TaskTopo taskTopo : learnedSmooth) {
			if (allMigratingTasks.contains(taskTopo)) {
			    migratingSmoothTasks.add(taskTopo);
			}
		    }
		}
		else
		{
		    ArrayList<Integer> tmp = new ArrayList<Integer>();
		    for (TaskTopo taskTopo : allMigratingTasks) {
			if (!ackers.contains(taskTopo)) {
			    tmp.add(taskTopo.getTaskId());
			}
		    }
		    manager.delRdyTasks();
		    migratingTasks = tmp;
		}

		/* Wait until all task are ready to migrate. Everyone has completed state and buffer backup */
		int count = 0;
		ArrayList<TaskTopo> rdyLearn = zkw.getReadyTask();
		long timeout = System.currentTimeMillis();
		while (true) {
		    for (TaskTopo taskTopo : zkw.getReadyTask()) {
			if (!rdyLearn.contains(taskTopo)) {
			    rdyLearn.add(taskTopo);
			}
		    }
		    if (rdyLearn.containsAll(migratingSmoothTasks) || System.currentTimeMillis() - timeout > MIGRATION_TIMEOUT) {
			Logging.append("Rdy task is: " + rdyLearn.toString(), Level.INFO, ScheduleFetcher.class);
			break;
		    }
		    if (rdyLearn.size() != count) {
			timeout = System.currentTimeMillis();
			count = rdyLearn.size();
			Logging.append("Rdy task is: " + rdyLearn.toString(), Level.INFO, ScheduleFetcher.class);
		    }
		    try {
			Thread.sleep(100);
		    } catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		}
		//reset the grace period
		lastassignment = new Date();
		rebalancing = false;
	    }

	}

	// Clear all slots, unchanged slots will be reassigned to the same place anyway.
	for (WorkerSlot port : cluster.getUsedSlots()) {
	    cluster.freeSlot(port);
	}

	/* Apply the computed assignment to the cluster, if unchanged the applied assignment wont cause any worker process termination */
	for (Assignment p : prep.getAssignment()) {
	    SupervisorDetails sd = cluster.getSupervisorById(p.getNodeId());
	    ArrayList<ExecutorDetails> toAssign = new ArrayList<ExecutorDetails>();
	    for (String id : p.getExecutorsIds()) {
		for (ExecutorDetails ed : topologies.getById(p.getTopoId()).getExecutors()) {
		    String completeId = "[" + ed.getStartTask() + " " + ed.getEndTask() + "]";
		    if (id.equals(completeId)) {
			toAssign.add(ed);
		    }
		}
	    }
	    if (p.getPort() != -1) {
		WorkerSlot ws = new WorkerSlot(sd.getId(), p.getPort());
		if (!toAssign.isEmpty()) {
		    Logging.append("confirming " + p.toString() + "   " + p.getPort(), Level.INFO, ScheduleFetcher.class);
		    cluster.assign(ws, p.getTopoId(), toAssign);
		}

	    } else {
		for (Integer free : sd.getAllPorts()) {
		    if (usedSlots.get(p.getNodeId()) == null || !usedSlots.get(p.getNodeId()).contains(free)) {
			WorkerSlot ws = new WorkerSlot(sd.getId(), free);

			p.setPort(free);
			ArrayList<Integer> slots = usedSlots.get(p.getNodeId());
			if (slots != null) {
			    slots.add(free);
			} else {
			    slots = new ArrayList<Integer>();
			    slots.add(free);
			    usedSlots.put(p.getNodeId(), slots);
			}
			if (!toAssign.isEmpty()) {
			    Logging.append("placing " + p.toString() + "   " + p.getPort(), Level.INFO, ScheduleFetcher.class);
			    lastassignment = new Date();
			    cluster.assign(ws, p.getTopoId(), toAssign);
			    NodesInLastAssignment = pr.getAvailableNodes();
			}
			break;
		    }
		}
	    }

	}
	//if not in grace period run scale-out scale-in procedure
	if (new Date().getTime() - lastassignment.getTime() > SCHEDULE_FETCH_TIME) {
	    applyScalingPolicy(topologies);
	}

	if (!rebalanceCommandsByTopo.entrySet().isEmpty()) {
	    //reset grace period
	    lastassignment = new Date();
	}

	/* print out new and old assignment */
	if (lastResp != null)
	    Logging.append("last plac was: " + lastResp.toString(), Level.INFO, ScheduleFetcher.class);

	if (lastResp != null)
	    Logging.append("new plac is: " + lastResp.toString(), Level.INFO, ScheduleFetcher.class);

	/* add new assignment to Mongo DB where it can be easily accessed by other system components */
	HashMap<String, String> nodeToHost = new HashMap<String, String>();
	for (SupervisorDetails sd : cluster.getSupervisors().values()) {
	    nodeToHost.put(sd.getId(), sd.getHost());
	}
	prep.setHostToNodeId(nodeToHost);
	manager.upsertDepl(prep);
	lastResp = prep;
	zkw.cleanUp();
	ArrayList<String> nodeids = new ArrayList<String>();
	for (NodeSlots ns : pr.getAvailableNodes()) {
	    nodeids.add(ns.getNodeId());
	}
	cleanDb(nodeids);

    }

    public static void cleanDb(ArrayList<String> availableNode) {
	Logging.append("Cleaning db ", Level.INFO, ScheduleFetcher.class);
	ArrayList<ExecutorTuple> executorDbList = manager.getExecutor(null);
	ArrayList<NodeTuple> nodes = manager.getNodes(null);
	for (ExecutorTuple executorTuple : executorDbList) {
	    String mongoId = executorTuple.getExecutorId();
	    String execId = mongoId.split("@")[0];
	    String topoId = mongoId.split("@")[1];
	    boolean exists = false;
	    for (Assignment assing : lastResp.getByTopology(topoId)) {
		if (assing.getExecutorsIds().contains(execId)) {
		    exists = true;
		    break;
		}
	    }
	    if (!exists) {
		manager.delExecutors(new ExecutorTuple(mongoId, 0, null));
		manager.delTraffic(new TrafficTuple(null, mongoId, null, 0));
		manager.delTraffic(new TrafficTuple(null, null, mongoId, 0));
	    }
	}
	for (NodeTuple nodeTuple : nodes) {
	    if (!availableNode.contains(nodeTuple.getNodeId())) {
		manager.delNode(new NodeTuple(nodeTuple.getNodeId(), 0, 0, 0, 0));
	    }
	}

    }

    public static void addOrIncrement(String comp, HashMap<String, Integer> componentExecutorCount) {
	if (componentExecutorCount.containsKey(comp)) {
	    componentExecutorCount.put(comp, componentExecutorCount.get(comp) + 1);
	} else {
	    componentExecutorCount.put(comp, 1);
	}
    }

    public void applyScalingPolicy(Topologies topologies) {

	Logging.append("Running scaling policy ", Level.INFO, ScheduleFetcher.class);

	// recupero le informazioni su tutti gli executor
	ArrayList<ExecutorTuple> executorInfo = manager.getExecutor(null);
	// recuper le informazioni sut tutti i nodi
	ArrayList<NodeTuple> nodeInfo = manager.getNode();
	for (TopologyDetails topo : topologies.getTopologies()) {

	    // tengo traccia dei componenti che hanno subito uno scaling
	    HashMap<String, Boolean> compScaled = new HashMap<String, Boolean>();
	    // hashmap dove conservare il numero di executor da usare per ciascun componente
	    HashMap<String, Integer> ComponentExecutorCount = new HashMap<String, Integer>();
	    // Cout degli executor iniziali di ciascun componente
	    HashMap<String, Integer> StartingComponentExecutorCount = new HashMap<String, Integer>();
	    // recupero dal deployment l`assignment attuale per questa topologia
	    ArrayList<Assignment> topoAssignments = lastResp.getByTopology(topo.getId());
	    // recupero il mapping tra executor e component per questa topologia
	    Map<ExecutorDetails, String> execToComp = topo.getExecutorToComponent();
	    HashMap<String, Integer> compMaxExec = new HashMap<String, Integer>();
	    // recupero la lista di tutti gli executor per questa topologia
	    Collection<ExecutorDetails> executors = topo.getExecutors();

	    Logging.append("running upscaling passage", Level.INFO, ScheduleFetcher.class);
	    // scorro tutti gli executor per il passaggio di upscaling
	    for (ExecutorDetails executor : executors) {
		// recupero il componente per questo executor
		String compId = execToComp.get(executor);
		Integer tasks = Math.max(executor.getEndTask() - executor.getStartTask(), 0);
		for (int i = 0; i <= tasks; i++) {
		    addOrIncrement(compId, compMaxExec);
		}

		// inizialmente un componente nasce come non scaled
		if (compScaled.get(compId) == null)
		    compScaled.put(compId, false);

		if (compId.equals("__acker")) {
		    Logging.append("upscaler skipping acker ", Level.INFO, ScheduleFetcher.class);
		    continue;
		}

		// incremento il numero di executor per tenero conto di questo executor
		addOrIncrement(compId, ComponentExecutorCount);
		addOrIncrement(compId, StartingComponentExecutorCount);
		String completeExeId = "[" + executor.getStartTask() + " " + executor.getEndTask() + "]";
		String nodeId = null;
		// recuper il nodo a cui e` assegnato l`executor
		for (Assignment assign : topoAssignments) {
		    if (assign.getExecutorsIds().contains(completeExeId)) {
			nodeId = assign.getNodeId();
			break;
		    }
		}
		int index = -1;
		// recupero le informazioni sul nodo e sull`executor
		if (nodeId != null)
		    index = nodeInfo.indexOf(new NodeTuple(nodeId, 0, 0, 0, 0));
		if (index == -1) {
		    Logging.append("node info is missing, skipping scale " + nodeId, Level.INFO, ScheduleFetcher.class);
		    rebalanceCommandsByTopo.clear();
		    return;
		}
		NodeTuple nt = nodeInfo.get(index);
		index = executorInfo.indexOf(new ExecutorTuple(completeExeId + "@" + topo.getId(), 0, new Date()));
		if (index == -1) {
		    Logging.append("executor info is missing, skipping scale " + completeExeId + "@" + topo.getId(), Level.INFO, ScheduleFetcher.class);
		    rebalanceCommandsByTopo.clear();
		    return;
		}
		ExecutorTuple et = executorInfo.get(index);

		// se questo executor sta occupando tutto il suo core con lo scaling factor allora aggiungo un executor
		if (et.getExecutorMhz() >= nt.getCoreMhz() * UPPER_SCALING_FACTOR && executor.getEndTask() - executor.getStartTask() > 0) {
		    Logging.append("upscaled component " + compId + " to: " + (ComponentExecutorCount.get(compId) + 1), Level.INFO, ScheduleFetcher.class);
		    addOrIncrement(compId, ComponentExecutorCount);
		    compScaled.put(compId, true);
		}
	    }
	    for (String comp : compScaled.keySet()) {
		if (compScaled.get(comp)) {
		    double oldc = StartingComponentExecutorCount.get(comp);
		    double newc = ComponentExecutorCount.get(comp);
		    double maxScaleFactor = ((double) (compMaxExec.get(comp) - newc)) / ((double) (compMaxExec.get(comp)));
		    if (((Math.abs(newc - oldc)) / oldc) < WEAK_SCALE_THR && newc < compMaxExec.get(comp)) {
			compScaled.put(comp, false);
			ComponentExecutorCount.put(comp, (int) oldc);
			Logging.append("Ignoring weak upscale comp: " + comp + " start scale: " + newc, Level.INFO, ScheduleFetcher.class);
		    } else if (maxScaleFactor <= SCALE_TO_MAX_THR) {
			Logging.append("Pushing up scale: " + comp + " start scale: " + newc, Level.INFO, ScheduleFetcher.class);
			compScaled.put(comp, true);
			ComponentExecutorCount.put(comp, compMaxExec.get(comp));
		    }
		}
	    }
	    Logging.append("running downscaling passage", Level.INFO, ScheduleFetcher.class);
	    ArrayList<String> evaluated = new ArrayList<String>();
	    // passaggio downscaling exeguito sui componenti
	    for (String compId : execToComp.values()) {
		if (!evaluated.contains(compId))
		    evaluated.add(compId);
		else
		    continue;
		// Se il componente ha subito upscaling lo salto
		if (compScaled.get(compId)) {
		    Logging.append("skipping upscaled component " + compId, Level.INFO, ScheduleFetcher.class);
		    continue;
		}
		if (compId.equals("__acker")) {
		    Logging.append("downscaler skipping acker ", Level.INFO, ScheduleFetcher.class);
		    continue;
		}
		// recupera tutti gli executor di questo componente
		ArrayList<ExecutorDetails> executorsOfThisComponent = new ArrayList<ExecutorDetails>();
		for (ExecutorDetails executorDetails : executors) {
		    if (execToComp.get(executorDetails).equals(compId))
			executorsOfThisComponent.add(executorDetails);
		}

		boolean downscale = true;
		// scorro tutta la lista degli executor su quel componente
		for (ExecutorDetails executorDetails : executorsOfThisComponent) {
		    String completeExeId = "[" + executorDetails.getStartTask() + " " + executorDetails.getEndTask() + "]";
		    String nodeId = null;
		    // recupero il nodo a cui e` assegnato l`executor
		    for (Assignment assign : topoAssignments) {
			if (assign.getExecutorsIds().contains(completeExeId)) {
			    nodeId = assign.getNodeId();
			    break;
			}
		    }
		    int index = -1;
		    // recupero le informazioni sul nodo e sull`executor
		    if (nodeId != null)
			index = nodeInfo.indexOf(new NodeTuple(nodeId, 0, 0, 0, 0));
		    if (index == -1) {
			Logging.append("node info is missing, skipping scale " + nodeId, Level.INFO, ScheduleFetcher.class);
			downscale = false;
			rebalanceCommandsByTopo.clear();
			return;
		    }
		    NodeTuple nt = nodeInfo.get(index);
		    index = executorInfo.indexOf(new ExecutorTuple(completeExeId + "@" + topo.getId(), 0, new Date()));
		    if (index == -1) {
			Logging.append("executor info is missing, skipping scale " + completeExeId + "@" + topo.getId(), Level.INFO,
				ScheduleFetcher.class);
			downscale = false;
			rebalanceCommandsByTopo.clear();
			return;
		    }
		    ExecutorTuple et = executorInfo.get(index);
		    // verifico se tutti gli executor se qualche executor non sta sotto la soglia
		    if (et.getExecutorMhz() >= nt.getCoreMhz() * LOWER_SCALING_FACTOR) {
			Logging.append("executor " + completeExeId + "@" + topo.getId() + " in " + compId + " is over the value " + nt.getCoreMhz()
				* LOWER_SCALING_FACTOR, Level.INFO, ScheduleFetcher.class);
			// non faccio downscale in questo caso
			downscale = false;
			break;
		    }

		}
		// se non ho trovato nessuno sopra la soglia
		if (downscale) {
		    Integer execNum = ComponentExecutorCount.get(compId);
		    if (execNum > 1) {

			// aggiorno il numero di executor
			int newExecutorCout = (int) ((double) execNum / (double) 2);
			int scaleFactor = MAX_DOWNSCALE_FACTOR;
			if (scaleFactor == 0) {
			    scaleFactor = lastResp.getComponentToTask().get(compId).size();
			}
			// verifico di non essere sceso sotto la soglia massima di downscale, nel qual caso imposto il minimo consentito
			if (newExecutorCout < lastResp.getComponentToTask().get(compId).size() / scaleFactor) {
			    newExecutorCout = lastResp.getComponentToTask().get(compId).size() / scaleFactor;
			}
			if (execNum != newExecutorCout) {
			    Logging.append("downscaled " + compId + " to: " + newExecutorCout, Level.INFO, ScheduleFetcher.class);
			    ComponentExecutorCount.put(compId, newExecutorCout);
			    // indico che il componente ha subito scaling
			    compScaled.put(compId, true);
			}

		    }
		}
	    }
	    // example rebalance command ./storm rebalance hello -w 0 -n 2 -e emitter=1 -e emitterBolt=1 -e closingBolt=1
	    boolean isAnyoneScaled = false;
	    for (Boolean scaled : compScaled.values()) {
		if (scaled)
		    isAnyoneScaled = true;
	    }
	    if (isAnyoneScaled) {
		String command = "/storm rebalance " + topo.getName() + " -w 0";
		for (String compId : ComponentExecutorCount.keySet()) {
		    command += " -e " + compId + "=" + ComponentExecutorCount.get(compId);
		}
		rebalanceCommandsByTopo.put(topo.getId(), command);
		Logging.append("command " + command, Level.INFO, ScheduleFetcher.class);

	    }

	}

    }

    public static PlacementResponse postRequest(String json, String URL) {
	try {

	    HttpClient httpClient = HttpClientBuilder.create().build();
	    HttpPost postRequest = new HttpPost(URL);

	    StringEntity input = new StringEntity(json);
	    input.setContentType("application/json");
	    postRequest.setEntity(input);

	    HttpResponse response = httpClient.execute(postRequest);

	    if (response.getStatusLine().getStatusCode() != 200) {
		Logging.append("Failed : HTTP error code : " + response.getStatusLine().getStatusCode(), Level.INFO, ScheduleFetcher.class);
	    }

	    BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

	    String output;
	    String content = "";

	    while ((output = br.readLine()) != null) {
		content += output;
	    }
	    httpClient.getConnectionManager().shutdown();
	    ObjectMapper mapper = new ObjectMapper();
	    return mapper.readValue(content, PlacementResponse.class);

	} catch (MalformedURLException e) {
	    Logging.append(e.getMessage(), Level.INFO, ScheduleFetcher.class);
	    e.printStackTrace();

	} catch (IOException e) {
	    Logging.append(e.getMessage(), Level.INFO, ScheduleFetcher.class);
	    e.printStackTrace();

	}
	return null;

    }

}
