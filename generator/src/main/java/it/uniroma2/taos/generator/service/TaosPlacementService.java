package it.uniroma2.taos.generator.service;

import it.uniroma2.taos.generator.controller.GeneratorConfHandler;
import it.uniroma2.taos.commons.dao.ExecutorTuple;
import it.uniroma2.taos.commons.dao.NodeTuple;
import it.uniroma2.taos.commons.dao.TrafficTuple;
import it.uniroma2.taos.commons.network.ExecutorTopo;
import it.uniroma2.taos.commons.network.NodeSlots;
import it.uniroma2.taos.commons.network.Assignment;
import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;
import it.uniroma2.taos.commons.persistence.MongoManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.annotation.Resource;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

@Component
public class TaosPlacementService implements PlacementService {
    private static final double gamma = GeneratorConfHandler.GAMMA;
    private static double worthness = 0;
    private ArrayList<TrafficStat> totalTraffic = new ArrayList<TaosPlacementService.TrafficStat>();
    private HashMap<String, ArrayList<TrafficTuple>> outgoingTraffic = new HashMap<String, ArrayList<TrafficTuple>>();
    private HashMap<String, ArrayList<TrafficTuple>> incomingTraffic = new HashMap<String, ArrayList<TrafficTuple>>();
    private HashMap<String, Double> executorMhz = new HashMap<String, Double>();
    private HashMap<String, NodeTuple> nodeSpec = new HashMap<String, NodeTuple>();
    private static final Logger logger = LogManager.getLogger(TaosPlacementService.class.getName());
    private static MongoManager mm = new MongoManager(GeneratorConfHandler.MONGO_IP, GeneratorConfHandler.MONGO_PORT);
    @Resource(name = "RRPlacementService")
    private RRPlacementService rrPlacementService;
    private PlacementResponse current = null;
    private int currentCost = 0;
    private Thread reporter = null;
    long timer = 0;

    class CostReporter implements Runnable {

	@Override
	public void run() {
	    while (true) {
		mm.putNetworkCost(currentCost);
		try {
		    Thread.sleep(5000);
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
	    }

	}
    }

    public PlacementResponse getPlacement(PlacementRequest pr) {
	if (reporter == null || !reporter.isAlive()) {
	    reporter = new Thread(new CostReporter());
	    reporter.start();
	}
	logger.info("---------------------------------------------------------");
	current = mm.getDepl();
	boolean haveInfo = true;
	totalTraffic.clear();
	outgoingTraffic.clear();
	synchronized (incomingTraffic) {
	    incomingTraffic.clear();
	}

	executorMhz.clear();
	ArrayList<ExecutorTopo> exeList = pr.getExecutors();
	logger.info("Request is : " + pr.toString());
	logger.info("Checking for occurred rebalance: ");
	HashMap<String, ArrayList<ExecutorTopo>> missing = new HashMap<String, ArrayList<ExecutorTopo>>();
	HashMap<String, ArrayList<ExecutorTopo>> news = new HashMap<String, ArrayList<ExecutorTopo>>();

	for (String topoId : pr.getReabalanceTopoIds()) {
	    ArrayList<ExecutorTopo> requestExecutors = pr.executorsByTopo(topoId);
	    ArrayList<ExecutorTopo> currentExecutor = new ArrayList<ExecutorTopo>();
	    for (Assignment assign : current.getByTopology(topoId)) {
		for (String execID : assign.getExecutorsIds()) {
		    currentExecutor.add(new ExecutorTopo(topoId, execID));
		}
	    }
	    ArrayList<ExecutorTopo> toAdd = new ArrayList<ExecutorTopo>();
	    for (ExecutorTopo misser : currentExecutor) {
		if (!requestExecutors.contains(misser)) {
		    toAdd.add(misser);
		}
	    }
	    missing.put(topoId, toAdd);
	    toAdd = new ArrayList<ExecutorTopo>();
	    for (ExecutorTopo newer : requestExecutors) {
		if (!currentExecutor.contains(newer)) {
		    toAdd.add(newer);
		}
	    }
	    news.put(topoId, toAdd);

	}
	logger.info("missing are: " + missing.toString());
	logger.info("news are: " + news.toString());

	for (ArrayList<ExecutorTopo> newers : news.values()) {
	    for (ExecutorTopo newer : newers) {
		pr.getExecutors().remove(newer);
	    }

	}
	for (ArrayList<ExecutorTopo> missers : missing.values()) {
	    for (ExecutorTopo misser : missers) {
		pr.getExecutors().add(misser);
	    }

	}
	logger.info("Topo with added missing and removed news  is: " + pr);

	logger.info("Getting executor resource usage");
	for (ExecutorTopo exe : exeList) {
	    ArrayList<TrafficTuple> tmp1 = mm.getTraffic(exe.getMongoId(), null);
	    ArrayList<TrafficTuple> tmp = new ArrayList<TrafficTuple>();
	    for (TrafficTuple trafficTuple : tmp1) {
		String id = trafficTuple.getSource();
		String topoid = id.split("@")[1];
		String exeid = id.split("@")[0];
		if (exeList.contains(new ExecutorTopo(topoid, exeid))) {
		    if ((new Date()).getTime() - trafficTuple.getDate().getTime() < 60000)
			tmp.add(trafficTuple);
		}
	    }
	    outgoingTraffic.put(exe.getMongoId(), tmp);

	    tmp = new ArrayList<TrafficTuple>();
	    tmp1 = mm.getTraffic(null, exe.getMongoId());
	    for (TrafficTuple trafficTuple : tmp1) {
		String id = trafficTuple.getSource();

		String topoid = id.split("@")[1];
		String exeid = id.split("@")[0];
		if (exeList.contains(new ExecutorTopo(topoid, exeid))) {
		    if ((new Date()).getTime() - trafficTuple.getDate().getTime() < 60000)
			tmp.add(trafficTuple);
		}
	    }
	    synchronized (tmp) {

		incomingTraffic.put(exe.getMongoId(), tmp);
	    }

	    int total = 0;
	    for (TrafficTuple tt : incomingTraffic.get(exe.getMongoId())) {
		total += tt.getCount();
	    }
	    for (TrafficTuple tt : outgoingTraffic.get(exe.getMongoId())) {
		total += tt.getCount();
	    }
	    totalTraffic.add(new TrafficStat(exe.getMongoId(), total));
	    ArrayList<ExecutorTuple> res = mm.getExecutor(exe.getMongoId());
	    if (res.size() == 0) {
		logger.info(exe.getMongoId());
		haveInfo = false;
		break;
	    }

	    ExecutorTuple et = mm.getExecutor(exe.getMongoId()).get(0);

	    executorMhz.put(exe.getMongoId(), et.getExecutorMhz());
	}
	PlacementResponse toReturn = new PlacementResponse();
	if (haveInfo == true) {

	    logger.info("Sorting executors by total traffic");
	    Collections.sort(totalTraffic, new Comparator<TrafficStat>() {
		@Override
		public int compare(TrafficStat o1, TrafficStat o2) {
		    if (o1.getTotalTraffic() < o2.getTotalTraffic())
			return 1;
		    if (o1.getTotalTraffic() > o2.getTotalTraffic())
			return -1;

		    return o1.getMongoId().compareTo(o2.getMongoId());
		}
	    });

	    logger.info("Getting node specifications");
	    for (NodeSlots node : pr.getAvailableNodes()) {
		NodeTuple nst = mm.getNode(node.getNodeId());
		if (nst == null) {
		    logger.info("node " + node.getNodeId() + " is not reporting information, ignoring");
		    continue;
		}
		// fix per amazon
		// nst.setCoreCount(4);
		nodeSpec.put(node.getNodeId(), nst);
	    }

	    logger.info("collected totalTraffic list: " + totalTraffic.toString());
	    logger.info("collected outgoingTraffic list: " + outgoingTraffic.toString());
	    logger.info("collected incomingTraffic list: " + incomingTraffic.toString());
	    logger.info("collected executorMhz list: " + executorMhz.toString());
	    logger.info("collected nodeSpec list: " + nodeSpec.toString());

	    logger.info("Running Xu Scheduler");
	    // Xu Algo Here
	    for (int i = 0; i < totalTraffic.size(); i++) {
		// Update Q properly according to the ongoing placement
		TrafficStat oExecutor = totalTraffic.get(i);
		logger.info(oExecutor.getExecutoId());
		ArrayList<String> Q = new ArrayList<String>();
		ArrayList<String> saturated = new ArrayList<String>();
		String topo = "";
		for (ExecutorTopo et : exeList) {
		    if (et.getMongoId().equals(oExecutor.getMongoId()))
			topo = et.getTopoId();
		}
		HashMap<String, Integer> counts = toReturn.countUsedSlots(topo);
//		NodeSlots debugChosen = pr.getAvailableNodes().get(0);
		for (NodeSlots n : pr.getAvailableNodes()) {
		    int usedSlots = 0;
		    if (counts.get(n.getNodeId()) != null)
			usedSlots = counts.get(n.getNodeId());
		    if (n.getAvailableSlots() - usedSlots <= 0) {
			logger.info("Node " + n.getNodeId() + " has no more slots, hence not feasible");
			continue;

		    }
		    NodeTuple nt = nodeSpec.get(n.getNodeId());
		    double currentScheduleContribution = 0;
		    for (ExecutorTopo ei : toReturn.getExecutorsOnNode(n.getNodeId())) {
			currentScheduleContribution += executorMhz.get(ei.getMongoId());
		    }

		    double expectedMhz = executorMhz.get(oExecutor.getMongoId()) + currentScheduleContribution;
		    if (expectedMhz > (nt.getCoreMhz() * nt.getCoreCount()) * nt.getSaturationFactor()) {
			logger.info("Node " + n.getNodeId() + " has no more MHz capacity, hence not feasible");
			saturated.add(n.getNodeId());
			continue;
		    }
//		    ArrayList<String> vips = new ArrayList<String>();
//		    vips.add("[26 27]");
//		    vips.add("[26 26]");
//		    vips.add("[27 27]");
//		    vips.add("[28 28]");
//		    if ((!n.equals(debugChosen) && vips.contains(oExecutor.getExecutoId()))
//			    || (n.equals(debugChosen) && !vips.contains(oExecutor.getExecutoId())))
//			continue;
		    Q.add(n.getNodeId());
		}

		int numberOfNodes = pr.getAvailableNodes().size();
		Collections.sort(Q, new Comparator<String>() {
		    @Override
		    public int compare(String o1, String o2) {
			NodeTuple o1t = nodeSpec.get(o1);
			NodeTuple o2t = nodeSpec.get(o2);
			if (((double) o1t.getCoreCount()) * o1t.getCoreMhz() * o1t.getSaturationFactor() < ((double) o2t.getCoreCount())
				* o2t.getCoreMhz() * o2t.getSaturationFactor())
			    return 1;
			if (((double) o1t.getCoreCount()) * o1t.getCoreMhz() * o1t.getSaturationFactor() > ((double) o2t.getCoreCount())
				* o2t.getCoreMhz() * o2t.getSaturationFactor())
			    return -1;
			return o1.compareTo(o2);
		    }
		});
		ArrayList<String> keep = new ArrayList<String>();
		for (String candidate : Q) {
		    int maxExecutors = (int) Math.ceil(gamma * ((double) exeList.size() / (double) numberOfNodes));

		    if (toReturn.getExecutorsOnNode(candidate).size() >= maxExecutors) {
			continue;
		    }

		    keep.add(candidate);
		}
		if (keep.isEmpty()) {
		    logger.info("No available node found, trying without gamma");
		    keep = Q;
		}
		if (keep.isEmpty()) {
		    logger.info("No available node found, trying without mhz constraint");
		    keep = saturated;

		}
		if (keep.isEmpty()) {
		    logger.info("There aren`t enough slots in the cluster, fatal!");
		    PlacementResponse res = new PlacementResponse();
		    return res;
		}
		Q = keep;
		int min = -1;
		String argmin = null;
		for (String feasNode : Q) {
		    int currVal = 0;
		    for (String node : toReturn.getPlacedNodes()) {
			if (feasNode.equals(node))
			    continue;
			for (TrafficTuple tt : incomingTraffic.get(oExecutor.getMongoId())) {
			    currVal += tt.getCount();
			}
			for (TrafficTuple tt : outgoingTraffic.get(oExecutor.getMongoId())) {
			    currVal += tt.getCount();
			}

		    }
		    if (min == -1) {
			min = currVal;
			argmin = feasNode;
		    } else if (currVal < min) {
			min = currVal;
			argmin = feasNode;
		    }

		}
		ExecutorTopo toAddExecutor = exeList.get(exeList.indexOf(new ExecutorTopo(oExecutor.getTopoId(), oExecutor.getExecutoId())));
		Assignment placem = toReturn.getPlacementByNodeAndTopo(argmin, toAddExecutor.getTopoId());
		if (placem == null) {

		    ArrayList<String> toAdd = new ArrayList<String>();
		    toAdd.add(toAddExecutor.getExeId());
		    placem = new Assignment(argmin, toAddExecutor.getTopoId(), toAdd);
		    toReturn.getAssignment().add(placem);
		} else {
		    placem.getExecutorsIds().add(toAddExecutor.getExeId());
		}

	    }
	} else {
	    logger.info("Some information are missing, switching to RoundRobin");
	    toReturn = rrPlacementService.getPlacement(pr);
	}
	if ((checkOverload(current) && !checkOverload(toReturn))) {
	    // if ((checkDeltDistr(current, pr) && !checkDeltDistr(toReturn, pr)) || (checkOverload(current) && !checkOverload(toReturn))) {
	    toReturn.setIgnoreIfDeployed(false);
	} else if (!validate(toReturn))
	    toReturn.setIgnoreIfDeployed(true);
	if(!pr.getReabalanceTopoIds().isEmpty())
	{
	    toReturn=current;
	    toReturn.setIgnoreIfDeployed(true);
	}

	for (String topoId : missing.keySet()) {
	    // recupero la lista degli assignment per questa topologia
	    ArrayList<Assignment> assignmentWithAddedMissing = toReturn.getByTopology(topoId);
	    // costruisco una mappa assignment---->lista task in quell`assignment
	    HashMap<Assignment, ArrayList<Integer>> assignmentTasks = new HashMap<Assignment, ArrayList<Integer>>();
	    for (Assignment assignment : assignmentWithAddedMissing) {
		ArrayList<Integer> tasks = new ArrayList<Integer>();
		for (String exeId : assignment.getExecutorsIds()) {
		    tasks.addAll(toReturn.getTasksByExecutorId(exeId));
		}
		assignmentTasks.put(assignment, tasks);
	    }
	    // per ciascuno dei nuovi executor scaturito dall`operazione di rebalance della suddetta topologia
	    for (ExecutorTopo newer : news.get(topoId)) {
		// Assignment migliore (massimo numero di matches)
		Assignment bestAssignment = null;
		int matches = 0;
		// recupero la lista dei task relativa al nuovo executor
		ArrayList<Integer> newtask = toReturn.getTasksByExecutorId(newer.getExeId());
		// scorro tutta la mappa assignment---->lista task
		for (Entry<Assignment, ArrayList<Integer>> e : assignmentTasks.entrySet()) {
		    int count = 0;
		    for (Integer i : newtask) {
			if (e.getValue().contains(i)) {
			    count++;
			}
		    }
		    if (Math.max(count, matches) == count) {
			matches = count;
			bestAssignment = e.getKey();
		    }
		}
		// assegno l`executor in gestine al bestAssignment
		bestAssignment.getExecutorsIds().add(newer.getExeId());
	    }
	    // scorro tutti i vecchi executor che ora sono missing, e li elimino dinuovo dalla topologia
	    for (ExecutorTopo misser : missing.get(topoId)) {
		for (Assignment assignment : assignmentWithAddedMissing) {
		    if (assignment.getExecutorsIds().contains(misser.getExeId())) {
			assignment.getExecutorsIds().remove(misser.getExeId());
		    }
		}
	    }
	}
	logger.info("Response is: " + toReturn.toString());
//	toReturn.setIgnoreIfDeployed(true);
	return toReturn;
    }

    public ArrayList<String> getChilds(ArrayList<ExecutorTopo> childs, ExecutorTopo parent) {
	ArrayList<String> toReturn = new ArrayList<String>();
	Integer parentLower = parent.getLowerIndex();
	Integer parentUpper = parent.getUpperIndex();
	for (ExecutorTopo child : childs) {
	    if (child.getLowerIndex() >= parentLower && child.getUpperIndex() <= parentUpper)
		toReturn.add(child.getExeId());
	}
	return toReturn;
    }

    public ExecutorTopo getParent(ArrayList<ExecutorTopo> parents, ExecutorTopo child) {
	Integer childLower = child.getLowerIndex();
	Integer childUpper = child.getUpperIndex();
	for (ExecutorTopo parent : parents) {
	    if (parent.getLowerIndex() <= childLower && parent.getUpperIndex() >= childUpper)
		return parent;
	}
	return null;
    }

    public boolean validate(PlacementResponse newResp) {

	if (current == null)
	    return true;
	logger.info("current " + current.toString());
	currentCost = computeCost(current);
	logger.info("currentCost " + currentCost);
	int futureCost = computeCost(newResp);
	logger.info("futureCost " + futureCost);
	if (futureCost > currentCost) {
	    worthness = 0;
	    return false;
	}
	if (currentCost == 0)
	    return false;
	double currentWorthness = ((double) Math.abs(currentCost - futureCost) / (double) currentCost);
	if (currentWorthness < worthness) {
	    worthness = currentWorthness;
	} else
	    worthness = worthness * (1 - GeneratorConfHandler.DAMPING_FACTOR) + currentWorthness * (GeneratorConfHandler.DAMPING_FACTOR);
	logger.info("percentage: " + worthness + " currentcost: " + currentCost + " futurecost: " + futureCost);
	if (worthness < GeneratorConfHandler.CONVENIENCE_FACTOR) {
	    return false;
	}
	return true;
    }

    public boolean checkOverload(PlacementResponse toCheck) {
	boolean returnTrue = false;
	long totalMhz = 0;
	long totalMaxMhz = 0;
	if (toCheck == null)
	    return false;
	for (String nodeId : nodeSpec.keySet()) {
	    ArrayList<Assignment> assignmentOnNode = new ArrayList<Assignment>();
	    for (Assignment assignment : toCheck.getAssignment()) {
		if (assignment.getNodeId().equals(nodeId))
		    assignmentOnNode.add(assignment);
	    }
	    for (Assignment as : assignmentOnNode) {
		NodeTuple nt = nodeSpec.get(as.getNodeId());
		if (nt == null) {
		    logger.info("node missing");
		    return false;
		}
		long maxMhz = (long) (nodeSpec.get(as.getNodeId()).getCoreMhz() * nodeSpec.get(as.getNodeId()).getCoreCount() * (nodeSpec.get(
			as.getNodeId()).getSaturationFactor() + GeneratorConfHandler.SATURATION_TOLLERANCE));
		long mhz = 0;
		for (String id : as.getExecutorsIds()) {
		    String completeid = id + "@" + as.getTopoId();
		    if (executorMhz.get(completeid) == null)
			return false;
		    mhz += executorMhz.get(completeid);
		}
		logger.info(mhz + " maxmhz: " + maxMhz);
		totalMhz += mhz;
		totalMaxMhz += maxMhz;
		if (mhz > maxMhz)
		    returnTrue = true;
	    }

	}

	if (returnTrue && !(Math.min(1.0, totalMhz / totalMaxMhz) > GeneratorConfHandler.SATURATION_THRESHOLD))
	    return true;// baseline put true back
	return false;
    }

    public int computeCost(PlacementResponse toCompute) {
	int cost = 0;
	for (Assignment pl : toCompute.getAssignment()) {
	    for (String eid : pl.getExecutorsIds()) {
		if (outgoingTraffic.get(eid + "@" + pl.getTopoId()) != null) {
		    for (TrafficTuple tt : outgoingTraffic.get(eid + "@" + pl.getTopoId())) {
			if (!pl.getExecutorsIds().contains(tt.getDestination().split("@")[0])) {
			    cost += tt.getCount();
			}
		    }
		}
		if (incomingTraffic.get(eid + "@" + pl.getTopoId()) != null) {
		    for (TrafficTuple tt : incomingTraffic.get(eid + "@" + pl.getTopoId())) {
			if (!pl.getExecutorsIds().contains(tt.getSource().split("@")[0])) {
			    cost += tt.getCount();
			}
		    }
		}
	    }
	}

	return cost/2;
    }

    class TrafficStat {
	private String mongoId;
	private int totalTraffic;

	public TrafficStat(String mongoId, int totalTraffic) {
	    super();
	    this.mongoId = mongoId;
	    this.totalTraffic = totalTraffic;
	}

	public String getExecutoId() {
	    return mongoId.split("@")[0];
	}

	public String getTopoId() {
	    return mongoId.split("@")[1];
	}

	public String getMongoId() {
	    return mongoId;
	}

	public void setMongoId(String mongoId) {
	    this.mongoId = mongoId;
	}

	public int getTotalTraffic() {
	    return totalTraffic;
	}

	public void setTotalTraffic(int totalTraffic) {
	    this.totalTraffic = totalTraffic;
	}

	@Override
	public String toString() {
	    return "{exeId: " + mongoId + " totalTraffic: " + totalTraffic + "}";
	}

    }
}
