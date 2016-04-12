package it.uniroma2.adaptivescheduler;

import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;

public class CentralizedCleaner {

	private static final String ZK_MIGRATION_DIR = "/extension/continuousscheduler/migrations";
	private static final String ZK_COORDINATES_DIR = "/extension/networkspace/coordinates";

	private static final int ROUND_BEFORE_DISAPPEAR = 3;
	private static boolean DEBUG = false;
	
	@SuppressWarnings("rawtypes")
	private Map config;

	private SimpleZookeeperClient zkClient = null; 

	private Map<String, Integer> nodeRegistry = new HashMap<String, Integer>();
	
	public CentralizedCleaner(){

	}
	
	
	@SuppressWarnings("rawtypes")
	public CentralizedCleaner(Map conf) {
		
		this.config = conf;
		
		readConfig(conf);
		
		/* Create managers */
		createZookeeperClient();
		
	}
	
	@SuppressWarnings("rawtypes") 
	private void readConfig(Map config){
		if (config != null){

		}
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

					if (DEBUG)
						System.out.println("Connecting to ZooKeeper");
					/* Initialization need to write to zookeeper. Wait until a connection is established */
					while(!zkClient.isConnected()){
						try {
							if (DEBUG)
								System.out.print(".");
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
					if (DEBUG){
						System.out.println();
						System.out.println("ZkClient Created!");						
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	

	public void cleanZKInformation(Topologies topologies, Cluster cluster) {
		
		if (DEBUG)
			System.out.println("Clean ZK Informations");
		
		if (zkClient == null)
			return;
		
		/* Remove deleted topologies from migrations */
		removeUnusefulTopologies(topologies, cluster);
		
		/* Remove disappeared host from network space */
		removeDisappearedNodes(topologies, cluster);
		
	}
	

	private void removeUnusefulTopologies(Topologies topologies, Cluster cluster){
		
		Collection<TopologyDetails> topologyDetailsSet = topologies.getTopologies();
		
		if (topologyDetailsSet == null)
			return;
		
		Iterator<TopologyDetails> itd = topologyDetailsSet.iterator();
		Set<String> activeTopologyIds = new HashSet<String>();
		
		while (itd.hasNext()){
			TopologyDetails topology = itd.next();
			activeTopologyIds.add(topology.getId());
		}
		
		if (!zkClient.exists(ZK_MIGRATION_DIR)){
			return;
		}
		
		List<String> migratingTopologyIds = zkClient.getChildren(ZK_MIGRATION_DIR);
		for(String migratingTid : migratingTopologyIds){
			if (!activeTopologyIds.contains(migratingTid)){
				zkClient.deleteRecursive(ZK_MIGRATION_DIR + "/" + migratingTid);
				if (DEBUG)
					System.out.println("Topology " + migratingTid + " deleted");
			}
		}
		
	}
	
	
	private void removeDisappearedNodes(Topologies topologies, Cluster cluster){
		
		Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
		
		if (supervisors == null)
			return;
		
		insertOrResetNodeToRegistry(supervisors);
		Set<String> disappearedSupervisorIds = updateNodeRegistryCountersAndGetDisappeared();

		if (!zkClient.exists(ZK_COORDINATES_DIR)){
			return;
		}
		
		List<String> supervisorsCoordinates = zkClient.getChildren(ZK_COORDINATES_DIR);
		for(String sc : supervisorsCoordinates){
			if (disappearedSupervisorIds.contains(sc)){
				zkClient.deleteRecursive(ZK_COORDINATES_DIR + "/" + sc);
				if (DEBUG)
					System.out.println("Coordinate of " + sc + " deleted");
			}
		}
	}
	
	private void insertOrResetNodeToRegistry(Map<String, SupervisorDetails> supervisors){
		/* Insert new supervisors or reset their counter */
		for(String supervisorId : supervisors.keySet()){
			
			Integer nCounter;
			if (nodeRegistry != null){
				nCounter = nodeRegistry.get(supervisorId);
			} 
			nCounter = new Integer(-1);
			nodeRegistry.put(supervisorId, nCounter);

		}		
	}

	private Set<String> updateNodeRegistryCountersAndGetDisappeared(){
		
		Set<String> disappeared = new HashSet<String>();

		if (nodeRegistry.isEmpty()){
			return disappeared;
		}
		
		Iterator<String> it = nodeRegistry.keySet().iterator();
		
		while(it.hasNext()){
			String sid = it.next();
			
			Integer nc = nodeRegistry.get(sid);
			int counter = nc.intValue() + 1;
			nc = new Integer(counter);
			
			if (counter > ROUND_BEFORE_DISAPPEAR){
				disappeared.add(sid);
				it.remove();
			}
		}		
		
		return disappeared;
	}

}
