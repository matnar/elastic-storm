package it.uniroma2.adaptivescheduler.scheduler;

import it.uniroma2.adaptivescheduler.entities.Node;
import it.uniroma2.adaptivescheduler.persistence.DatabaseException;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.persistence.entities.Measurement;
import it.uniroma2.adaptivescheduler.scheduler.internal.AugmentedExecutorDetails;
import it.uniroma2.adaptivescheduler.scheduler.internal.RelatedComponentDetails;
import it.uniroma2.adaptivescheduler.scheduler.internal.RelatedComponentDetails.Type;
import it.uniroma2.adaptivescheduler.space.KNNItem;
import it.uniroma2.adaptivescheduler.space.KNearestNodes;
import it.uniroma2.adaptivescheduler.space.LatencyPlusOneSpace;
import it.uniroma2.adaptivescheduler.space.Point;
import it.uniroma2.adaptivescheduler.space.SimpleKNearestNodes;
import it.uniroma2.adaptivescheduler.space.Space;
import it.uniroma2.adaptivescheduler.space.SpaceFactory;
import it.uniroma2.adaptivescheduler.space.SpringForce;
import it.uniroma2.adaptivescheduler.utils.CPUMonitor;
import it.uniroma2.adaptivescheduler.vivaldi.QoSMonitor;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.GeneralTopologyContext;

import com.google.gson.Gson;

public class AdaptiveScheduler {

	private double SPRING_FORCE_THRESHOLD = 1.0;
	private double SPRING_FORCE_DELTA = 0.1;
	private int K_NEAREST_NODE_TO_RETRIEVE = 5;
	private int MAX_EXECUTOR_PER_SLOT = 4;
	private double MIGRATION_THRESHOLD = 0.10;
	
	/* Const */
	private static final String ZK_MIGRATION_DIR = "/extension/continuousscheduler/migrations";
	private int COOLDOWN_ROUNDS = 5;
	private boolean DEBUG = true;
	
	private boolean JUST_WATCH = false;
	
	private ISupervisor supervisor;
	private String nodeId;
	private SimpleZookeeperClient zkClient; 
	private DatabaseManager databaseManager; 

	private QoSMonitor networkSpaceManager;
	
	private Map<String, Integer> cooldownBuffer;
	private List<String> localTopologyComponentMigrations;
	
	@SuppressWarnings("rawtypes")
	public AdaptiveScheduler(ISupervisor supervisor, SimpleZookeeperClient zkClient, 
			DatabaseManager databaseManager, QoSMonitor networkSpaceManager, Map config) {
		super();
		this.supervisor = supervisor;
		this.nodeId = this.supervisor.getSupervisorId();
		this.zkClient = zkClient;
		this.databaseManager = databaseManager;
		this.networkSpaceManager = networkSpaceManager;
		cooldownBuffer = new HashMap<String, Integer>();
		localTopologyComponentMigrations = new ArrayList<String>();
		if (config != null){
			readConfig(config);
			setCostSpaceParameters(config);
		}
	}
	
	@SuppressWarnings("rawtypes") 
	private void readConfig(Map config){
		
		if (config != null){
			
			Double dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_FORCE_THRESHOLD);
			if (dValue != null){
				SPRING_FORCE_THRESHOLD = dValue.doubleValue();
				System.out.println("Read spring force threshold: " + SPRING_FORCE_THRESHOLD);
			}
			
			dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_FORCE_DELTA);
			if (dValue != null){
				SPRING_FORCE_DELTA = dValue.doubleValue();
				System.out.println("Read delta: " + SPRING_FORCE_DELTA);
			}
			
			dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_MIGRATION_THRESHOLD);
			if (dValue != null){
				MIGRATION_THRESHOLD = dValue.doubleValue();
				System.out.println("Read migration threshold: " + MIGRATION_THRESHOLD);
			}
			
			Integer iValue = (Integer) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_K_NEAREST_NODE);
			if (iValue != null){
				K_NEAREST_NODE_TO_RETRIEVE = iValue.intValue();
				System.out.println("Nearest node to retrieve (K): " + K_NEAREST_NODE_TO_RETRIEVE);
			}
			
			iValue = (Integer) config.get(Config.ADAPTIVE_SCHEDULER_CONTINUOUS_SCHEDULER_MAX_EXECUTOR_PER_SLOT);
			if (iValue != null){
				MAX_EXECUTOR_PER_SLOT = iValue.intValue();
				System.out.println("Max executor per slot: " + MAX_EXECUTOR_PER_SLOT);
			}
			

			Boolean bValue = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_JUST_MONITOR);
			if (bValue != null){
				JUST_WATCH = bValue.booleanValue();
				System.out.println("Just watch: " + JUST_WATCH);
			}
			
		}
		
	}
	
	
	@SuppressWarnings("rawtypes")
	private void setCostSpaceParameters(Map config){
		
		double latency1 = 1;
		double w1 = 1;
		double w2 = 1;
		double w3 = 0;

		
		Double dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_MAX_LATENCY);
		if (dValue != null){
			if (dValue.doubleValue() == 0.0)
				dValue = new Double(1.0);
			
			latency1 = 1.0 / dValue.doubleValue();
		}

		dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_W1);
		if (dValue != null){
			w1 = dValue.doubleValue();
		}
		dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_W2);
		if (dValue != null){
			w2 = dValue.doubleValue();
		}
		dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_W3);
		if (dValue != null){
			w3 = dValue.doubleValue();
		}

		Space s = SpaceFactory.createSpace();
		if (s instanceof LatencyPlusOneSpace){
			System.out.println("Updating Space Normalization Factor(s): [2+1] " + w1 * latency1 + ", " + w2 * latency1 + ", " + w3);
			
			LatencyPlusOneSpace.setWeights(new double[]{w1 * latency1, w2 * latency1, w3});
		}

	}
	
	/**
	 * Initialize Scheduler
	 * 
	 * Nodes on zookeeper are created
	 */
	public void initialize(){
		
		System.out.println("Initializing Continuous Scheduler");
		
		/* Initialization */
		initializeZKNode();
		
		try {
			databaseManager.initializeOnSupervisor();
		} catch (DatabaseException e) {
			e.printStackTrace();
		} catch (SQLException e){
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}
	
	/**
	 * Initialize nodes on zookeeper
	 */
	private void initializeZKNode(){
		if (zkClient != null){
			try {
				zkClient.mkdirs(ZK_MIGRATION_DIR);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
		}
	}

	

	
	public void schedule(Topologies topologies, 
			Map<String, GeneralTopologyContext> topologiesContext, 
			Cluster cluster){
		
		Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
		Map<String, Node> knownNodes = networkSpaceManager.copyKnownNodesCoordinate();

		/* Remove not assigned topologies from cooldown buffer*/
		cleanupCooldownCounter(assignments.keySet());

		/* Save utilization on file */
		saveUtilizationOnFile();
		
		/* For each topology */
		for (String tid : assignments.keySet()){
			System.out.println("Selected topology: " + tid);
			TopologyDetails topology = topologies.getById(tid);
			GeneralTopologyContext topologyContext = topologiesContext.get(tid);
			
			if (!canReschedule(tid)){
				System.out.println("Current topology is cooling down and cannot be scheduled yet");
				incrementCooldownCounter(tid);
				continue;
			}
			
			System.out.println("[1/4] Monitor phase...");
			List<AugmentedExecutorDetails> augmentedExecutors = monitor(tid, topology, topologyContext, cluster);
			
			if (augmentedExecutors == null){
				System.out.println("No local executor to reschedule");
				return;
			}
				
			/* DEBUG */
			System.out.println(". Candidate components: ");
			for (AugmentedExecutorDetails e : augmentedExecutors){
				System.out.println(". . " + e.getComponentId() + " " + e.getExecutor());
			}
						
			for (AugmentedExecutorDetails e : augmentedExecutors){

				if(!canReschedule(tid)){
					System.out.println("Current topology cannot be scheduled anymore, maybe a component as been reassigned");
					
					unsetMigration(tid, e.getComponentId());
					continue;
				}
				
				
				if (JUST_WATCH){
					unsetMigration(tid, e.getComponentId());
					continue;
				}
				
				System.out.println("[2/4] Analyze phase...");
				Point newExecutorPosition = analyze(e, tid, topology, topologyContext, cluster, knownNodes);
			
				System.out.println("[3/4] Plan phase...");
				WorkerSlot candidateSlot = plan(newExecutorPosition, e, tid, topology, topologyContext, cluster, knownNodes);
				
				System.out.println("[4/4] Execute phase...");
				execute(e, tid, candidateSlot, cluster);
				
			}
			
		}
		
		
	}
	
	/**
	 * Feedback control loop: monitor
	 * 
	 * Collect information of executors which can be re-assigned. Parent 
	 * and child components are retrieved thanks to the topologyContext.
	 * 
	 * @param topologyId
	 * @param topology
	 * @param cluster
	 * @param topologyContext
	 * @return
	 */
	private List<AugmentedExecutorDetails> monitor(String topologyId, TopologyDetails topology, 
			GeneralTopologyContext topologyContext, Cluster cluster){
	
		Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
		if (assignments == null)
			return null;
		
		SchedulerAssignment assignment = assignments.get(topologyId);
		if (assignment == null)
			return null;
		
		Map<ExecutorDetails, WorkerSlot> executorToSlot = assignment.getExecutorToSlot();
		if (executorToSlot == null)
			return null;
		
		Map<ExecutorDetails, WorkerSlot> localExecutorToSlot = new HashMap<ExecutorDetails, WorkerSlot>();
		List<AugmentedExecutorDetails> candidateExecutors = new ArrayList<AugmentedExecutorDetails>();

		/* 1. Retrieve local executors 
		 * Topologies contains all topologies with at least a local executor */
		for(ExecutorDetails e : executorToSlot.keySet()){
			
			WorkerSlot slot = executorToSlot.get(e);

			if (slot == null)
				continue;
			
			if (nodeId.equals(slot.getNodeId())){
				localExecutorToSlot.put(e, slot);
			}
		}
		

		System.out.println("Local Executors: ");
		/* 2. Retrieve parent and child components */	
		/* 3. Check if any of them is already involved in a migration */
		for(ExecutorDetails e : localExecutorToSlot.keySet()){
			WorkerSlot slot = localExecutorToSlot.get(e);
			
			System.out.println(". . " + e + " - slot: " + slot.getPort());

			/* Check if current component is migrating */
			String componentId = topology.getExecutorToComponent().get(e);
			
			/* skip system components */
			if (componentId == null || componentId.startsWith("__"))
				continue;
			
			if (isMigrating(topologyId, componentId)){
				continue;
			}

			boolean aRelatedIsMigrating = false;
			boolean unpinnedComponent = true;

			/* Retrieve child and check if someone is migrating */
			List<String> targetComponentsId = getTargetComponentsId(topologyContext, componentId);
			if (targetComponentsId == null)
				continue;
			
			for (String target : targetComponentsId){
				
				aRelatedIsMigrating = isMigrating(topologyId, target);
				
				if (!target.startsWith("__")){
					unpinnedComponent = false;
				}
				
				if (aRelatedIsMigrating){
					break;
				}
			}

			if (aRelatedIsMigrating){
				/* A target component is migrating, the current one cannot migrate */
				continue;
			}
			
			if (unpinnedComponent){
				/* All target components start with __ (system components): current component is a "leaf" of the graph */
				continue;
			}

			
			/* Retrieve parent and check if someone is migrating */
			List<String> sourceComponentsId = getSourceComponentsId(topologyContext, componentId);
			if (sourceComponentsId == null)
				continue;
			
			unpinnedComponent = true;
			for (String src : sourceComponentsId){

				aRelatedIsMigrating = isMigrating(topologyId, src);
				
				if (!src.startsWith("__")){
					unpinnedComponent = false;
				}
				
				if (aRelatedIsMigrating){
					break;
				}
			}

			if (aRelatedIsMigrating){
				/* A source component is migrating, the current one cannot migrate */
				continue;
			}
			
			if (unpinnedComponent){
				/* All source components start with __ (system components): current component is a "root" of the graph */
				continue;
			}

			/* Add executor to the list of candidate ones */
			AugmentedExecutorDetails aExecutor = new AugmentedExecutorDetails(e, componentId);
			aExecutor.setWorkerSlot(slot);
			aExecutor.setTargetComponentsId(targetComponentsId);
			aExecutor.setSourceComponentsId(sourceComponentsId);
		
			/* Set component migration for current component */
			setMigration(topologyId, componentId);

			candidateExecutors.add(aExecutor);
		}

		return candidateExecutors;
		
	}
	
	
	
	
	private Point analyze(AugmentedExecutorDetails augmentedExecutors, 
			String topologyId, TopologyDetails topology, GeneralTopologyContext topologyContext,
			Cluster cluster, Map<String, Node> networkSpaceNodes){
		
		return analyze(augmentedExecutors, topologyId, topology, topologyContext, 
				cluster, networkSpaceNodes, networkSpaceManager.getCoordinates(), 
				supervisor.getSupervisorId());

	}
	
	
	
	
	private Point analyze(AugmentedExecutorDetails augmentedExecutors, 
			String topologyId, TopologyDetails topology, GeneralTopologyContext topologyContext,
			Cluster cluster, Map<String, Node> networkSpaceNodes, 
			Point initialExecutorCoordinates, 
			String currentNodeId){
		
		/* For current executor (augmentedExecutors): 
		 * 1. determine parent and child components with their position
		 * 2. for each compute exchanged data rates 
		 * 4. compute spring force and the new executor position 
		 */
		
		SchedulerAssignment assignment = cluster.getAssignmentById(topologyId);
		
		/* 1. Retrieve parent and child worker slot and relative network space coordinates */
		List<String> parentComponents = augmentedExecutors.getSourceComponentsId();
		List<RelatedComponentDetails> parentComponentsDetails = 
				getRelatedComponentsDetails(networkSpaceNodes, topology, topologyContext, assignment, parentComponents, Type.PARENT);

		List<String> childComponents = augmentedExecutors.getTargetComponentsId();
		List<RelatedComponentDetails> childComponentsDetails = 
				getRelatedComponentsDetails(networkSpaceNodes, topology, topologyContext, assignment, childComponents, Type.CHILD);

		
		/* 2. Compute exchanged data rates */
		Map<String, Double> destinationDataRates = new HashMap<String, Double>();
		Map<String, Double> sourceDataRates = new HashMap<String, Double>();

		List<Integer> localTask = getTasksFromExecutor(augmentedExecutors.getExecutor());
		
		destinationDataRates = getWorkerNodeDatarate(topologyId, localTask, true, childComponentsDetails);
		sourceDataRates = getWorkerNodeDatarate(topologyId, localTask, false, parentComponentsDetails);
		
		
		/* 3. compute spring force and the new executor position */
		/* 3.1 initial executor position */
		Point executorNewCoordinates = computeInitialExecutorCoordinates(initialExecutorCoordinates); 
		
		SpringForce f = null;
		int statCounter = 0;
		int forcedExit = 10000;
		if(DEBUG)
			System.out.println(". Current coordinates: " + executorNewCoordinates);

		do{
			/* 3.2 set spring force to 0 */
			f = new SpringForce();

			/* 3.3 for each parent component (in every worker slot) compute and sum the spring force 
					(spring force = latency * datarate) */
			SpringForce parentComponentsForce = computeComponentsForce(executorNewCoordinates, parentComponentsDetails, sourceDataRates, currentNodeId);
			/* if a force with magnitude 0 is added, the resulting force is smoothed */
			if (!parentComponentsForce.isNull())
				f.add(parentComponentsForce);
			
			/* 3.4 for each child component (in every worker slot) compute and sum the spring force  
					(spring force = latency * datarate) */
			SpringForce childComponentsForce = computeComponentsForce(executorNewCoordinates, childComponentsDetails, destinationDataRates, currentNodeId);
			if (!childComponentsForce.isNull())
				f.add(childComponentsForce);
			
			/* 3.5 update executor position */
			executorNewCoordinates =  f.movePoint(executorNewCoordinates, SPRING_FORCE_DELTA);
			
			statCounter++;
			forcedExit--;
		}while(f != null && !f.lessThan(SPRING_FORCE_THRESHOLD) && forcedExit > 0);

		if(DEBUG)
			System.out.println(". . Final position into the network space: " + executorNewCoordinates + " (iterations: " + statCounter +")");
		return executorNewCoordinates;
	}
	
	
	private WorkerSlot plan(Point newExecutorPosition, AugmentedExecutorDetails augmentedExecutors, 
			String topologyId, TopologyDetails topology, GeneralTopologyContext topologyContext, Cluster cluster, Map<String, Node> networkSpaceNodes){
		
		WorkerSlot candidateSlot = null; 
		if (newExecutorPosition == null){
			return null;
		}
		
		List<KNNItem> kNearestNode = getKNearestNodes(K_NEAREST_NODE_TO_RETRIEVE, newExecutorPosition, networkSpaceNodes);
		
		/* Check returned list */
		if (kNearestNode == null || kNearestNode.size() == 0){
			return null;
		}

		double currentNodeDistance = getCurrentNodeDistance(kNearestNode);

		/* Find nearest worker slot */
		Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
		if (supervisors == null)
			return null;
		
        for (KNNItem candidateItem: kNearestNode){

        	Node n = candidateItem.getNode();
        	if (n == null)
        		continue;
        	
        	String candidate = n.getSupervisorId();
        	if (candidate == null)
        		continue;
        	
			SupervisorDetails candidateSupervisor = supervisors.get(candidate);
			
			if (candidateSupervisor == null){
				System.out.println(". Supervisor details is null");
				continue;
			}else{
				
				/* Check if the i-th near node is the current one */
				if (supervisor.getSupervisorId().equals(candidateSupervisor.getId())){
					if(DEBUG)
						System.out.println(". Current node is the best one");
					return null;								
				}
    		
				/* Check if the candidate node relative distance is above the migration threshold */
				if(relativeDistanceIsBelowMigrationThreshold(currentNodeDistance, candidateItem.getDistance())){
					if(DEBUG)
						System.out.println(". Relative distance of the candidate node is not above the migration threshold: dcur" + currentNodeDistance + " dcan" + candidateItem.getDistance());
					return null;								
				}

				/* Check if migration is useful by looking ahead*/
				if (!lookAheadAndCheckIfMigrationIsCovenient(candidateItem, augmentedExecutors, topologyId, 
						topology, topologyContext, cluster, networkSpaceNodes)){
					
					if(DEBUG)
						System.out.println(". Migration is useless... Current node is the best placement computed at the candidate node");
					return null;								
				}
				
				if(DEBUG){
					System.out.println(". Candidate: ID: " + candidateSupervisor.getId() + " host: " + candidateSupervisor.getHost() + 
						" [Ports: " + candidateSupervisor.getAllPorts().toString() + "]");
				}
    			
            	/* Try to re-use worker slot of candidate node which execute current topology */
            	WorkerSlot reusableWorkerSlot = currentTopologyBestWorkerSlotOnCandidateNode(topologyId, candidateSupervisor, cluster);
            	if (reusableWorkerSlot != null){
            		if(DEBUG)
            			System.out.println(". On candidate node there is a worker slot currently running this topology. Re-using worker slot");
            		return reusableWorkerSlot;
            	}
            	
    			/* Find available slot on nearest node */
    		    List<WorkerSlot> availableSlotsOnCandidateNode = cluster.getAvailableSlots(candidateSupervisor);
//            	System.out.println(". Available slots: " + availableSlotsOnCandidateNode);
            	
    		    if (availableSlotsOnCandidateNode == null || availableSlotsOnCandidateNode.isEmpty()){
    		    	// System.out.println(". There are no available slots on supervisor " + candidate + "... ");
    		    	continue;
    		    } else {
    		    	
    		    	candidateSlot = availableSlotsOnCandidateNode.get(0);
    		    	if (candidateSlot == null){
    		    		continue;
    		    	} else {
    		    		return candidateSlot;
    		    	}
    		    }    
            }
		}
        
        return candidateSlot;
	}
	
	
	private void execute(AugmentedExecutorDetails augmentedExecutors, 
			String topologyId, WorkerSlot candidateSlot, Cluster cluster){
		
		if (candidateSlot == null){
			unsetMigration(topologyId, augmentedExecutors.getComponentId());
	    	return;
		}
		
		SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologyId);
		if (currentAssignment == null)
			return;
		
        Map<ExecutorDetails, WorkerSlot> executorToSlots = currentAssignment.getExecutorToSlot();
        if (executorToSlots == null)
			return;
		
    	/* Prepare object to call cluster.freeSlot() */
    	ExecutorDetails executorDetails = augmentedExecutors.getExecutor();
    	if (executorDetails == null){
    		System.out.println("Executor details is null");
    		return;
    	}
    	WorkerSlot currentExecutorWorkerSlot = executorToSlots.get(executorDetails);
    	Set<WorkerSlot> workerSlotToFree = new HashSet<WorkerSlot>();
    	if (currentExecutorWorkerSlot != null)
    		workerSlotToFree.add(currentExecutorWorkerSlot);
   		workerSlotToFree.add(candidateSlot);
    	
    	
    	/* Prepare object to call cluster.assign() */
    	List<ExecutorDetails> executorsToReassign = new ArrayList<ExecutorDetails>();
    	executorsToReassign.add(executorDetails);
    	/* retrieve all pre-existent executor into the candidate worker slot */
    	for(ExecutorDetails e : executorToSlots.keySet()){
    		WorkerSlot ews = executorToSlots.get(e);
    		if (ews != null && candidateSlot.equals(ews)){
    			executorsToReassign.add(e) ;   			
    		}
    	}
    	
		/* Retrieve all executors running on local worker slot to reassign them */
		List<ExecutorDetails> executorsToConfirm = new ArrayList<ExecutorDetails>();
    	for(ExecutorDetails e : executorToSlots.keySet()){
    		if (executorToSlots.get(e) != null && executorToSlots.get(e).equals(currentExecutorWorkerSlot)){
    			if (!e.equals(executorDetails)){
    				executorsToConfirm.add(e);
    			}
    		}
    	}
		
		
		
    	/* Free WorkerSlots */
		cluster.freeSlots(workerSlotToFree);
    	
    	/* Reassign Executor */
		if (!executorsToConfirm.isEmpty()){
			cluster.assign(currentExecutorWorkerSlot, topologyId, executorsToConfirm);
		}
		if (!executorsToReassign.isEmpty()){
			cluster.assign(candidateSlot, topologyId, executorsToReassign);
		}
		
		/* NOTE: the effective migration is differred */
		unsetMigration(topologyId, augmentedExecutors.getComponentId());
		
		resetCooldownCounter(topologyId);
		cleanExecutorsData(topologyId, executorsToReassign);
		
		System.out.println("NEW ASSIGNMENT! We've assigned executors:" + executorsToReassign + " to slot: "
    			+ "[" + candidateSlot.getNodeId() + ", " + candidateSlot.getPort() + "]");
		System.out.println("NEW ASSIGNMENT! We've confirmed executors:" + executorsToConfirm + " to slot: "
    			+ "[" + currentExecutorWorkerSlot.getNodeId() + ", " + currentExecutorWorkerSlot.getPort() + "]");
    	
	}
	
	private Point computeInitialExecutorCoordinates(Point initialExecutorCoordinates){
		
		Space space = networkSpaceManager.getSpace();
		
		for(int i = space.getLatencyDimensions(); i < space.getTotalDimensions(); i++)
			initialExecutorCoordinates.set(i, 0.0);
		
		return initialExecutorCoordinates;
		
	}

	private WorkerSlot currentTopologyBestWorkerSlotOnCandidateNode(String topologyId, 
			SupervisorDetails candidateSupervisor, Cluster cluster){
		
		List<WorkerSlot> candidateWS = cluster.getAssignableSlots(candidateSupervisor);
		
		SchedulerAssignment assignment = cluster.getAssignmentById(topologyId);
		
		Map<WorkerSlot, Integer> slotCount = new HashMap<WorkerSlot, Integer>();
		
		for(WorkerSlot ws : candidateWS){
			
			for(ExecutorDetails e : assignment.getExecutorToSlot().keySet()){
				WorkerSlot ews = assignment.getExecutorToSlot().get(e);
				
				if (ws.equals(ews)){
					
					
					Integer c = slotCount.get(ws);
					if (c == null)
						c = new Integer(1);
					else
						c = new Integer(c.intValue() + 1);
					
					slotCount.put(ws, c);
				}
			}
		}
		
		WorkerSlot candidate = null;
		int bestCounter = Integer.MAX_VALUE;
		for(WorkerSlot ws : slotCount.keySet()){
						
			if (slotCount.get(ws) != null && slotCount.get(ws).intValue() < bestCounter){
				candidate = ws;
				bestCounter = slotCount.get(ws).intValue();
			}
		}
		
		if (bestCounter + 1 > MAX_EXECUTOR_PER_SLOT)
			candidate = null;
		
		return candidate;
		
	}
	
	private double getCurrentNodeDistance(List<KNNItem> kNearestNode){
		
		double distance = Double.MAX_VALUE;

		for(KNNItem item : kNearestNode){
			
			Node n = item.getNode();
			if (n == null)
				continue;
			
			if(n.getSupervisorId() != null && n.getSupervisorId().equals(supervisor.getSupervisorId())){
				distance = item.getDistance();
				break;
			}
		}
		
		return distance;
		
	}
	
	private boolean relativeDistanceIsBelowMigrationThreshold(double currentNodeDistance, double candidateNodeDistance){
		
		double relativeDistance = 0.0; 
		
		if (candidateNodeDistance + currentNodeDistance > 0.0)
			relativeDistance = Math.abs((currentNodeDistance - candidateNodeDistance)) / (candidateNodeDistance + currentNodeDistance);
		
		if (relativeDistance <= MIGRATION_THRESHOLD)
			return true;
		
		return false;
		
	}
	
	private boolean lookAheadAndCheckIfMigrationIsCovenient(KNNItem candidate, 
			AugmentedExecutorDetails augmentedExecutors, 
			String topologyId, TopologyDetails topology, GeneralTopologyContext topologyContext,
			Cluster cluster, Map<String, Node> networkSpaceNodes){
		
		Node candidateNode = candidate.getNode();
		if (candidateNode == null)
			return false;
		
		Point newCandidateItem = analyze(augmentedExecutors, topologyId, topology, 
				topologyContext, cluster, networkSpaceNodes, 
				candidateNode.getCoordinates(), candidateNode.getSupervisorId());
		
		
		List<KNNItem> kNearestNode = getKNearestNodes(K_NEAREST_NODE_TO_RETRIEVE, newCandidateItem, networkSpaceNodes);
		
		/* Check returned list */
		if (kNearestNode == null || kNearestNode.size() == 0){
			return false;
		}

        for (KNNItem candidateItem: kNearestNode){

        	Node n = candidateItem.getNode();
        	if (n == null)
        		continue;
        	
        	String candidateId = n.getSupervisorId();
        	if (candidateId == null)
        		continue;
        	
			/* Check if the best position for destination node is the current one */
			if (supervisor.getSupervisorId().equals(candidateId)){
				return false;								
			} else {
				return true;
			}
        }	
		
		return false;
	}
	
	private void cleanExecutorsData(String topologyId, List<ExecutorDetails> executors){
		
		if (executors == null)
			return;
		
		for (ExecutorDetails executor : executors){

			List<Integer> tasks = getTasksFromExecutor(executor);
			
			for (Integer task : tasks){
				try {
					databaseManager.deleteMeasurements(task.toString(), topologyId);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	
	private boolean canReschedule(String topologyId){

		Integer cooldownRounds = cooldownBuffer.get(topologyId);
		if (cooldownRounds != null && cooldownRounds.intValue() < COOLDOWN_ROUNDS){
			if (DEBUG)
				System.out.println("Topology " + topologyId + " cannot be rescheduled");
			return false;
		}

		if (cooldownRounds != null && cooldownRounds.intValue() >= COOLDOWN_ROUNDS){
			cooldownBuffer.remove(topologyId);
		}

//		System.out.println("Topology " + topologyId + " is going to be rescheduled");
		return true;
		
	}
	
	private void incrementCooldownCounter(String topologyId){
		
		Integer cooldownRounds = cooldownBuffer.get(topologyId);
		if (cooldownRounds == null){
			cooldownRounds = new Integer(0);
		} else {
			cooldownRounds = new Integer(cooldownRounds.intValue() + 1);
		}
		
		cooldownBuffer.put(topologyId, cooldownRounds);

	}
	
	private void resetCooldownCounter(String topologyId){
		cooldownBuffer.put(topologyId, new Integer(0));
	}
	
	private void cleanupCooldownCounter(Set<String> topologies){
	
		Set<String> tidToRemove = new HashSet<String>(cooldownBuffer.keySet());
		
		if (tidToRemove == null || tidToRemove.isEmpty())
			return;
		
		for (String tid : topologies){
			tidToRemove.remove(tid);
		}
		
		for (String tid : tidToRemove){
			cooldownBuffer.remove(tid);
		}	
			
	}

	
	
	private List<KNNItem> getKNearestNodes(int k, Point newExecutorPosition, Map<String, Node> networkSpaceNodes){
		
		if (networkSpaceManager == null)
			return null;
		
		KNearestNodes knn = new SimpleKNearestNodes(networkSpaceManager.getSpace());

		return knn.getKNearestNode(k, newExecutorPosition, networkSpaceNodes);
		
	}
	
	/*
	 * source indicates if task is the source
	 */
	private Map<String, Double> getWorkerNodeDatarate(String topologyId, List<Integer> localTask, boolean source, List<RelatedComponentDetails> relatedComponents){
		
		Map<String, Double> workerNodeDataRates = new HashMap<String, Double>();

		for(RelatedComponentDetails relatedComponent : relatedComponents){
			
			List<Integer> destinationTasks = relatedComponent.getTask();
			double avgDataratePerSingleWorkerSlot = 0.0;
		
			for (Integer task : localTask){
				for (Integer otherTask : destinationTasks){
					
					try {
						List<Measurement> measurements = null;
						
						if (source){
							measurements = databaseManager.getMeasurements(task.toString(), otherTask.toString(), topologyId);
						}else{
							measurements = databaseManager.getMeasurements(otherTask.toString(), task.toString(), topologyId);
						}
						
						/* There must be just a single measurement for each pair (taskFrom, taskTo) on current node */
						if (measurements != null && measurements.size() > 0){
							avgDataratePerSingleWorkerSlot += measurements.get(0).getValue();
						}
					} catch (SQLException e) {
						e.printStackTrace();
					} catch (DatabaseException e) {
						e.printStackTrace();
					}
					
				}
			}
			
			if (relatedComponent.getWorkerSlots() != null && relatedComponent.getWorkerSlots().size() > 0){
				avgDataratePerSingleWorkerSlot = avgDataratePerSingleWorkerSlot / ((double) relatedComponent.getWorkerSlots().size());
				
				for (WorkerSlot ws : relatedComponent.getWorkerSlots()){
				
					if (ws == null)
						continue;
					
				 	Double drPerSlot = workerNodeDataRates.get(ws.getNodeId());
					if (drPerSlot == null)
						drPerSlot = new Double(0.0);
				 	drPerSlot = new Double(drPerSlot.doubleValue() + avgDataratePerSingleWorkerSlot);
					
				 	workerNodeDataRates.put(ws.getNodeId(), drPerSlot);
				}
			}
		}
		
		return workerNodeDataRates;
	}
	
	private SpringForce computeComponentsForce(Point myCoordinates, List<RelatedComponentDetails> relatedComponents, 
			Map<String, Double> workerNodeDataRates, 
			String currentNodeId){
		
		SpringForce f = new SpringForce();
		
		if (relatedComponents == null || workerNodeDataRates == null)
			return f;
		
		for(RelatedComponentDetails relatedComponent : relatedComponents){
			
			for (WorkerSlot slot : relatedComponent.getWorkerSlots()){
				
				if (slot == null)
					continue;
				
				String otherNodeId = slot.getNodeId();
								
				if (currentNodeId == null || currentNodeId.equals(otherNodeId)){
					/* the other component is on the same node */
					continue;
				}
				
				Node otherNode = relatedComponent.getNetworkSpaceCoordinates().get(otherNodeId);
				if (otherNode == null)
					continue;
				
				Double datarate = workerNodeDataRates.get(otherNodeId);
				if (datarate == null)
					datarate = new Double(0.0);
				
				SpringForce cf = new SpringForce(otherNode.getCoordinates(), myCoordinates, datarate);
				f.add(cf);
				
			}
		}

		return f;		
	}
	
	private Node getWorkerSlotCoordinates(Map<String, Node> knownNetworkSpaceNodes, WorkerSlot workerSlot){

		/* 
		 * Retrieve node position following the steps: 
		 *  - search between nodes known by the network space manager
		 *  - if unknown, query zookeeper
		 *  - if unknown, return null 
		 * */
	
		if (workerSlot == null)
			return null;
		
		String nodeId = workerSlot.getNodeId();
		
		Node coordinates = knownNetworkSpaceNodes.get(nodeId);
		
		if (coordinates == null){
			coordinates = networkSpaceManager.retrieveCoordinatesFromZK(nodeId);
			if (coordinates != null)
				knownNetworkSpaceNodes.put(coordinates.getSupervisorId(), coordinates);
		}

		return coordinates;
	}
	
	private List<Integer> getTasksFromExecutor(ExecutorDetails executor){
		
		List<Integer> tasks = new ArrayList<Integer>();
		
		if (executor == null)
			return tasks;
		
		for (int i = executor.getStartTask(); i < executor.getEndTask() + 1; i++){
			tasks.add(new Integer(i));
		}
		
		return tasks;
	}
	
	
	private List<RelatedComponentDetails> getRelatedComponentsDetails(
			Map<String, Node> networkSpaceNodes, TopologyDetails topology, GeneralTopologyContext topologyContext,
			SchedulerAssignment assignment, List<String> relatedComponents, Type type){

		List<RelatedComponentDetails> relatedComponentsDetails = new ArrayList<RelatedComponentDetails>();

		for(String relatedComponentId : relatedComponents){
			RelatedComponentDetails relatedComponent = new RelatedComponentDetails(relatedComponentId, type);
			
			/* Retrieve related component worker slot */
			List<WorkerSlot> relatedComponentWorkerSlots = getWorkerSlotsFromComponentId(assignment, topology, relatedComponentId);
			relatedComponent.setWorkerSlots(relatedComponentWorkerSlots);
			
			/* Retrieve network space coordinates of the worker slot */
			Map<String, Node> parentNodes = new HashMap<String, Node>();
			for (WorkerSlot ws : relatedComponentWorkerSlots){
				Node n = getWorkerSlotCoordinates(networkSpaceNodes, ws);
				if (n != null)
					parentNodes.put(n.getSupervisorId(), n);
			}
			relatedComponent.setNetworkSpaceCoordinates(parentNodes);
			
			/* Retrieve component tasks */
			List<Integer> relatedComponentTasks = topologyContext.getComponentTasks(relatedComponentId);
			relatedComponent.setTask(relatedComponentTasks);
			
			relatedComponentsDetails.add(relatedComponent);
				
		}	
		
		return relatedComponentsDetails;
	}
	
	
	private List<WorkerSlot> getWorkerSlotsFromComponentId(SchedulerAssignment assignment, TopologyDetails topology, String componentId){
		
		Map<ExecutorDetails, WorkerSlot> executorToSlot = assignment.getExecutorToSlot();
		Map<ExecutorDetails, String> executorToComponent = topology.getExecutorToComponent();

		List<WorkerSlot> slots = new ArrayList<WorkerSlot>();

		if (executorToSlot == null || executorToComponent == null)
			return slots;
		
		for (ExecutorDetails e : executorToComponent.keySet()){
			if (executorToComponent.get(e) != null && executorToComponent.get(e).equals(componentId)){
				slots.add(executorToSlot.get(e));
			}
		}
		
		return slots;
	}
	
	private boolean isMigrating(String stormId, String componentId){

		String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;
	
		boolean zkMigration = zkClient.exists(dirname);
		
		if (zkMigration){
			for(String localMigration : localTopologyComponentMigrations){
				/* If component is migrating by current node, don't consider its migration*/
				if (localMigration.equals(getLocalTopologyMigrationId(stormId, componentId))){
					return false;
				}
			}
			
			return true;
		}else{
			return false;
		}
	}

	/* XXX: we should save the inode indicating {component, worker node id}, to guarantee no to "starve" if 
	 * a worker node fails and an executor needs to be moved */
	private void setMigration(String stormId, String componentId){

		String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;
		
		try {
			zkClient.mkdirs(dirname);
			
			localTopologyComponentMigrations.add(getLocalTopologyMigrationId(stormId, componentId));
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	private void unsetMigration(String stormId, String componentId){

		String dirname = ZK_MIGRATION_DIR + "/" + stormId + "/" + componentId;
		
		zkClient.deleteRecursive(dirname);

		localTopologyComponentMigrations.remove(getLocalTopologyMigrationId(stormId, componentId));
		
	}
	
	private String getLocalTopologyMigrationId(String topologyId, String componentId){
		return topologyId + ":::" + componentId;		
	}
	
	private List<String> getTargetComponentsId(GeneralTopologyContext context, String componentId){
		
		List<String> targetComponentsId = new ArrayList<String>();
		
		Map<String, Map<String, Grouping>> targets = context.getTargets(componentId);
		
		for(String streamId : targets.keySet()){
			Set<String> componentsId = targets.get(streamId).keySet();

			if (streamId!=null && streamId.equals("default"))
				targetComponentsId.addAll(componentsId);
		}
		
		return targetComponentsId;
	}
	
	private List<String> getSourceComponentsId(GeneralTopologyContext context, String componentId){
		
		List<String> sourceComponentsId = new ArrayList<String>();

		Map<GlobalStreamId, Grouping> sources = context.getSources(componentId);
		for(GlobalStreamId sid : sources.keySet()){
			if (sid!=null && !sid.get_componentId().startsWith("__"))
				sourceComponentsId.add(sid.get_componentId());
		}
		
		return sourceComponentsId;

	}

	

	
	
	private void saveUtilizationOnFile(){
		BufferedWriter bfile = null;
		PrintWriter outputFile = null; 
		double cpuusage = 0;

		try {
			bfile = new BufferedWriter(new FileWriter("log_" + supervisor.getSupervisorId() + ".txt", true));
			outputFile = new PrintWriter(bfile);

			try{ cpuusage = CPUMonitor.cpuUsage(); }catch(Exception e){}
			
			String message = System.currentTimeMillis() + ", " + cpuusage + ", " + networkSpaceManager.usingExtendedSpace();
			System.out.println("-- " + message + " -- ");
			
			outputFile.write(message);
			outputFile.flush();
			
		} catch (IOException e) { e.printStackTrace(); }
		try { if (bfile != null) {	bfile.close(); }
			  if (outputFile != null) { outputFile.close(); }		
		} catch (IOException e) { e.printStackTrace(); }
	
	}
	
}
