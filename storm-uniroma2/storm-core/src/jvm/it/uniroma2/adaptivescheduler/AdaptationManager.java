package it.uniroma2.adaptivescheduler;

import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.scheduler.AdaptiveScheduler;
import it.uniroma2.adaptivescheduler.space.SpaceFactory;
import it.uniroma2.adaptivescheduler.vivaldi.QoSMonitor;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.utils.Utils;


public class AdaptationManager {
	
	/**
	 * Descriptor of current node (it's seen as a supervisor)
	 */
	private ISupervisor supervisor;
	
	private QoSMonitor networkSpaceManager; 
	
	private SimpleZookeeperClient zkClient;

	private DatabaseManager databaseManager;
	
	private AdaptiveScheduler continuousScheduler;
	
	@SuppressWarnings("rawtypes")
	private Map config; 
	
	
	
	public AdaptationManager() {
		supervisor = null;
		config = null;
		
		networkSpaceManager = null;
		continuousScheduler = null;
	}
	
	public AdaptationManager(ISupervisor supervisor) {
		super();
		this.supervisor = supervisor;

		config = null; 
		networkSpaceManager = null;
		continuousScheduler = null;
 
		System.out.println("Adaptation Manager created!");
	}

	/**
	 * Start the adaptation manager. 
	 * 
	 * This function is called from supervisor.clj/mk-supervisor in a synchronous way. 
	 * A fast start-up should be performed, deferring slow operations in a dedicated thread. 
	 */
	public void initialize(){

		/* Read configuration */
		config = Utils.readStormConfig();
		
		/* Create managers */
		createZookeeperClient();
		
		if (supervisor != null){
			/* Start h2 db server */
			try {
			
				databaseManager = new DatabaseManager(getDatabasePath(), supervisor.getAssignmentId());
				Integer i = (Integer) config.get(Config.ADAPTIVE_SCHEDULER_INTERNAL_DATABASE_PORT);
				databaseManager.startServer(i.intValue());
			
				System.out.println("H2 database started");
			} catch (ClassNotFoundException e1) {
				e1.printStackTrace();
			} catch (SQLException e){
				e.printStackTrace();
			}

			Boolean extendedSpace = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_USE_EXTENDED_SPACE);
			if (extendedSpace != null && extendedSpace.booleanValue() == true)
				SpaceFactory.setUseExtendedSpace(true);

			networkSpaceManager = new QoSMonitor(supervisor.getSupervisorId(), zkClient, config);
			continuousScheduler = new AdaptiveScheduler(supervisor, zkClient, databaseManager, networkSpaceManager, config);
			
			/* Initialize managers */
			networkSpaceManager.initialize();
			continuousScheduler.initialize();
		}
	}
		

	private String getDatabasePath(){
		String dbBaseDir = "./";

		if (config != null){
			dbBaseDir = (String) config.get(Config.STORM_LOCAL_DIR);
		
			if (dbBaseDir != null){
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
		

	@SuppressWarnings("unchecked")
	private void createZookeeperClient(){
		
		/* Set default value */
		zkClient = null;

		/* ZKClient can be created only if configuration file has been read correctly */
		if (config != null){
			try {
				
				/* Retrieve ZK connection parameters */
				Integer port = (Integer) config.get(Config.STORM_ZOOKEEPER_PORT);
				Object obj = config.get(Config.STORM_ZOOKEEPER_SERVERS);
				if (obj instanceof List){
					List<String> servers = (List<String>) obj;
				
					/* Create ZK client
					 * NOTE: connection is done asynchronously */
					zkClient = new SimpleZookeeperClient(servers, port);

					// DEBUG: 
					System.out.println("Connecting to ZooKeeper");
					/* Initialization need to write to zookeeper. Wait until a connection is established */
					while(!zkClient.isConnected()){
						try {
							// DEBUG: 
							System.out.print(".");
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					// DEBUG: 
					System.out.println();
					System.out.println("ZkClient Created!");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	
	/* This function is used in mk-supervisor (function: shutdown) */
	public void stop(){
		networkSpaceManager.stop();
	}
	
	/**
	 * Update Network Space. 
	 * This function is called periodically from Supervisor to update the network space. 
	 * Period is determinated by the parameter <em>adaptivescheduler.network_space.round.duration</em>
	 * 
	 * @param currentSupervisors updated list of supervisors 
	 */
	public void updateNetworkSpace(Map<String, SupervisorDetails> currentSupervisors){
		
		// DEBUG
		System.out.println("Update network space called!");
		
		/* Create a list of other supervisors */
		List<SupervisorDetails> others = new ArrayList<SupervisorDetails>();
		for (String supervisorId : currentSupervisors.keySet()){
			
			/* Skip itself */
			if (supervisor != null && 
					supervisor.getSupervisorId() != null && 
					supervisor.getSupervisorId().equals(supervisorId))
				continue;
			
			SupervisorDetails otherSupervisor = currentSupervisors.get(supervisorId);
			
			if (otherSupervisor != null)
				others.add(otherSupervisor);
		}
		

		/* Update nodes the space manager can use */
		networkSpaceManager.updateNodes(others);

		/* Update the network space */
		networkSpaceManager.executeSingleRound();
		
	}

	
	

	
	/**
	 * Execute Scheduler. 
	 * This function is called periodically from Supervisor to update the network space. 
	 * Period is determinated by the parameter <em>adaptivescheduler.continuous_scheduler.freq.sec</em>
	 * 
	 */
	public void executeContinuousScheduler(Topologies topologies, Cluster cluster, Map<String, GeneralTopologyContext> topologiesContext){
		System.out.println("Execute Continuous Scheduler called!");
		
		if (topologies == null || cluster == null){
			return;
		}
		if (continuousScheduler != null){
			continuousScheduler.schedule(topologies, topologiesContext, cluster);
		}
		
	}


}
