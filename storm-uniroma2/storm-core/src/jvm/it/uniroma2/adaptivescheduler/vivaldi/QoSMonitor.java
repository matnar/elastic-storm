package it.uniroma2.adaptivescheduler.vivaldi;

import it.uniroma2.adaptivescheduler.entities.Node;
import it.uniroma2.adaptivescheduler.space.Point;
import it.uniroma2.adaptivescheduler.space.Serializer;
import it.uniroma2.adaptivescheduler.space.Space;
import it.uniroma2.adaptivescheduler.space.SpaceFactory;
import it.uniroma2.adaptivescheduler.utils.CPUMonitor;
import it.uniroma2.adaptivescheduler.zk.SimpleZookeeperClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.KeeperException;

import backtype.storm.Config;
import backtype.storm.scheduler.SupervisorDetails;

import com.google.gson.Gson;


public class QoSMonitor {
	
	/* Default values. They will be updated using configuration information */
	private static double ALPHA = 0.5;
	private static double BETA = 0.5; /* between 0 and 1 */
	private static int SERVER_PORT = 9110;
	private static double CONFIDENCE_THRESHOLD = 0.70;
	private static int ROUND_BETWEEN_PUBLICATION = 1;
	private static boolean EXTENDED_SPACE = false;
	private static boolean EXTEND_SPACE_WITH_UTILIZATION = false;
	
	
	private static final boolean NS_DEBUG = false;
	private static final String RELIABILITY_FILE = "reliability";
	
	/* Const */
	private static final String ZK_COORDINATES_DIR = "/extension/networkspace/coordinates";
	
	/* Current node id */
	private String supervisorId;
	/* Current coordinates into the network space */
	private Point coordinates; 
 	private ReadWriteLock coordinatesLock;
	
	/* Current prediction error */
	private double predictionError;
	/* Space used by the Vivaldi algorithm which implements operations between points */
	private Space networkSpace; 
	
	/* Other nodes into the network space */
	private Map<String, Node> nodesWithKnownPosition;
 	private Lock nodesWithKnownPositionLock;
	
	/* Store measurement received from NetworkSpaceServer */
	private Map<String, Node> measuredLatency; 
	private Lock measuredLatencyLock;
	
	/* List of supervisors currently active into the network 
	 * (it's updated using informations coming from ZooKeeper) */
	private List<SupervisorDetails> supervisors;
	private Lock supervisorsLock;
	
	/* Network Space Server is used to exchange coordinates and
	 * measure lantency between nodes */
	private NetworkSpaceServer server;
	private Thread serverThread;

	/* Zookeeper Client */
	private SimpleZookeeperClient zkClient;
	
	private Random rnd;

	private int roundFromLastPublication;
	
	private Map<String, Object> config;

	public QoSMonitor(String id) {
		this(id, null, null);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" }) 
	public QoSMonitor(String id, SimpleZookeeperClient zooKeeperClient, Map config) {
	
		this.config = (Map<String, Object>) config;
		
		networkSpace = SpaceFactory.createSpace();
		
		coordinates = new Point(networkSpace.getTotalDimensions());
		predictionError = 1.0;
		supervisorId = id;
		coordinatesLock = new ReentrantReadWriteLock();
	
		supervisors = new ArrayList<SupervisorDetails>();
		supervisorsLock = new ReentrantLock();

		measuredLatency = new HashMap<String, Node>();
		measuredLatencyLock = new ReentrantLock();
		
		roundFromLastPublication = ROUND_BETWEEN_PUBLICATION;

		nodesWithKnownPosition = new HashMap<String, Node>();
		nodesWithKnownPositionLock = new ReentrantLock();
		
		zkClient = zooKeeperClient;

		rnd = new Random();

		if (config != null){
			readConfig(config);
			saveReliabilityOnFile(config);
			
		}
		
		server = new NetworkSpaceServer(SERVER_PORT, this);
		serverThread = new Thread(server);

	}
	
	@SuppressWarnings("rawtypes") 
	private void readConfig(Map config){
		
		if (config != null){
			
			Double dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_NETWORK_SPACE_ALPHA);
			if (dValue != null){
				ALPHA = dValue.doubleValue();
				System.out.println("Read alpha: " + ALPHA);
			}
			
			dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_NETWORK_SPACE_BETA);
			if (dValue != null){
				BETA = dValue.doubleValue();
				System.out.println("Read beta: " + BETA);
			}
			
			Integer iValue = (Integer) config.get(Config.ADAPTIVE_SCHEDULER_NETWORK_SPACE_SERVER_PORT);
			if (iValue != null){
				SERVER_PORT = iValue.intValue();
				System.out.println("Read server port: " + SERVER_PORT);

			}
			
			dValue = (Double) config.get(Config.ADAPTIVE_SCHEDULER_NETWORK_SPACE_CONFIDENCE_THRESHOLD);
			if (dValue != null){
				CONFIDENCE_THRESHOLD = dValue.doubleValue();
				System.out.println("Read confidence threshold: " + CONFIDENCE_THRESHOLD);
			}
			
			iValue = (Integer) config.get(Config.ADAPTIVE_SCHEDULER_NETWORK_SPACE_ROUND_BEETWEEN_PUBLICATION);
			if (iValue != null){
				ROUND_BETWEEN_PUBLICATION = iValue.intValue();
				System.out.println("Read round between publications: " + ROUND_BETWEEN_PUBLICATION);
			}
			
			Boolean bValue = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_USE_EXTENDED_SPACE);
			if (bValue != null){
				EXTENDED_SPACE = bValue.booleanValue();
				System.out.println("Use extended space: " + EXTENDED_SPACE);
			}

			bValue = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_USE_UTILIZATION);
			if (bValue != null){
				EXTEND_SPACE_WITH_UTILIZATION = bValue.booleanValue();
				System.out.println("Use node utilization as third dimension: "  +  EXTEND_SPACE_WITH_UTILIZATION);
				System.out.println("Use node availability as third dimension: " + !EXTEND_SPACE_WITH_UTILIZATION);
			}
			
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void saveReliabilityOnFile(Map config){
		/*
		 * This function reads the node reliability from storm.yaml and saves it on a RELIABILITY_FILE.
		 * The RELIABILITY_FILE allow to change node reliability at runtime; 
		 * this file is also used in Utils/readReliability
		 */
		Boolean useExtendedSpace = (Boolean) config.get(Config.ADAPTIVE_SCHEDULER_USE_EXTENDED_SPACE);
		if (useExtendedSpace == null){
			return;
		}
		
		Double reliability = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_RELIABILITY);
		if (reliability != null){
			double reliabilityValue = reliability.doubleValue();
			
			if (reliabilityValue > 100)
				reliabilityValue = 100;
			
			File f = new File(RELIABILITY_FILE);
			BufferedWriter buffer;
			try {
				buffer = new BufferedWriter(new FileWriter(f));
				buffer.write("" + reliabilityValue);
				buffer.close();		
				System.out.println("Reliability value (" + reliabilityValue + "%) written on file");
			} catch (IOException e) { }
		}
	}
	
	public double getNodeReliability(){
		
		double reliability = 1;
		
		File f = new File(RELIABILITY_FILE);
		BufferedReader buffer;
		try {
			buffer = new BufferedReader(new FileReader(f));
			String read = buffer.readLine();
			if (read != null){
				reliability = Double.parseDouble(read) / 100.0;
				buffer.close();					
			} else {
				Double configReliability = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_RELIABILITY);
				
				if (configReliability != null){
					reliability = configReliability.doubleValue() / 100.0;
				}
			}
		} catch (Exception e) {
			Double configReliability = (Double) config.get(Config.ADAPTIVE_SCHEDULER_SPACE_RELIABILITY);
			
			if (configReliability != null){
				reliability = configReliability.doubleValue() / 100.0;
			}
		}
		
		return reliability;
	}
	
	public double getNodeUtilization(){

		double cpuusage = 0;
		try{
			cpuusage = CPUMonitor.cpuUsage();
		}catch(Exception e){}
		
		return cpuusage;
	}
	
	/**
	 * Initialize Network Space Manager
	 * 
	 * A network space server is started and nodes on zookeeper are initialized
	 */
	public void initialize(){
		
		System.out.println("Initializing Network Space Manager");
		
		/* Initialization */
		serverThread.start();
		
		/* Initialize node on zookeeper */
		initializeZKNode();
		
	}
	
	/**
	 * Initialize nodes on zookeeper
	 */
	private void initializeZKNode(){
		if (zkClient != null){
			try {
				zkClient.mkdirs(ZK_COORDINATES_DIR);
				zkClient.setData(getMyZKCoordinateNode(), Serializer.serializeCoordinates(coordinates, predictionError));
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
		}
	}
	
	
	/** 
	 * Terminate network space manager, cleaning up resources 
	 */
	public void stop(){

		System.out.println("Terminating Network Space Manager...");

		/* Remove node from zookeeper */
		System.out.println("[1/2] Removing nodes from ZooKeeper");
		removeZKNode();
		
		/* Stop server */
		System.out.println("[2/2] Terminating NetworkSpace Server");
		server.terminate();
		try {
			serverThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Remove node from zookeeper
	 */
	private void removeZKNode(){
		if (zkClient != null){
			zkClient.deleteRecursive(getMyZKCoordinateNode());
		}
	}
	
	/**
	 * Update the list of supervisor currently active into the network and 
	 * remove disappeared nodes from them having known position. 
	 * 
	 * This information is used by the Vivaldi algorithm. 
	 * 
	 * @param supervisors
	 */
	public void updateNodes(List<SupervisorDetails> supervisors){
		
		supervisorsLock.lock();
		try{
			this.supervisors.clear();

			if (supervisors == null || supervisors.isEmpty()){
				nodesWithKnownPositionLock.lock();
				try{
					nodesWithKnownPosition.clear();
				}finally{
					nodesWithKnownPositionLock.unlock();
				}
				/* Code in the finally block is executed */
				return;
			}

			nodesWithKnownPositionLock.lock();
			try{
				Set<String> disappearedNodes = nodesWithKnownPosition.keySet();
				
				for(SupervisorDetails sv : supervisors){
					/* Remove alive node from them considered disappeared */
					if (sv.getId() != null){
						disappearedNodes.remove(sv.getId());
					}
					
					/* Update the list of supervisors */
					this.supervisors.add(sv);
				}
				
				for(String supervisorId : disappearedNodes){
					nodesWithKnownPosition.remove(supervisorId);
				}
			}finally{
				nodesWithKnownPositionLock.unlock();
			}
						
		}finally{
			supervisorsLock.unlock();
		}
	}

	/**
	 * Insert node to a list of measurement that will be used to
	 * update coordinates of current node (see. Vivaldi algorithm).
	 * 
	 * @param node
	 */
	public void addLatencyMeasurement(Node node){
		if(node == null)
			return;
		
		measuredLatencyLock.lock();
		try{
			measuredLatency.put(node.getSupervisorId(), node);
		}finally{
			measuredLatencyLock.unlock();
		}

	}
	
	
	/**
	 * Retrive node coordinates from Zookeeper 
	 * 
	 * @param nodeId
	 * @return
	 */
	public Node retrieveCoordinatesFromZK(String nodeId){
		
		String path = ZK_COORDINATES_DIR + "/" + nodeId;

		byte[] data = zkClient.getData(path);
		
		if (data == null)
			return null;
		else
			return Serializer.deserializeCoordinates(nodeId, data);
	}

	
	
	/**
	 * Read all pending measurements and clean up the list. 
	 * 
	 * @return
	 */
	private List<Node> flushLatencyMeasurements(){

		List<Node> ml = new ArrayList<Node>();
		
		measuredLatencyLock.lock();
		try{
			if(measuredLatency.isEmpty())
				return ml;
			
			for (String key : measuredLatency.keySet()){
				
				Node n = measuredLatency.get(key);
				
				if (n != null){
					ml.add(n);
				}
			}
			measuredLatency.clear();
			
		}finally{
			measuredLatencyLock.unlock();
		}

		return ml;

	}

	
	/**
	 * Single round of the Vivaldi algorithm execution 
	 * 
	 * This function is composed like a single iteration of a MAPE cycle; 
	 * three phases are executed: 
	 *  - Monitor: measures latency with other nodes of the network
	 *  - Analyze: computes vivaldi force and prediction error from samples
	 *  - Update: updates node coordinates and prediction error; if the coordinates
	 *  			are predicted with low error rate, they are published on ZooKeeper
	 *  
	 */
	public void executeSingleRound() {

		/* *** Phase1: Monitor *** */ 
		/* Flush pending measurements (they are received from the network space server) */
		List<Node> sampledNodes = flushLatencyMeasurements();
		if (sampledNodes == null){
			sampledNodes = new ArrayList<Node>();
		}
		
		/* Select randomly a node and measure latency between current node and the selected one */
		Node activelySampledNode = getNode();
		if (activelySampledNode == null){
			if (NS_DEBUG){
				System.out.println("Other node is null");
			}
		}else{
			long latency = getLatency(activelySampledNode);
			activelySampledNode.setLastMeasuredLatency(latency);
			/* Add this node to the list of measurements */
			sampledNodes.remove(activelySampledNode);
			sampledNodes.add(activelySampledNode);
		}
		
		for(Node sn : sampledNodes){
			coordinatesLock.writeLock().lock();
			try{
				
				if (sn.getLastMeasuredLatency() == -1){
					if(NS_DEBUG)
						System.out.println("Invalid sample for node " + sn.getSupervisorId());
					continue;					
				}
					
				/* *** Phase2: Analyze *** */ 
				VivaldiForce f = new VivaldiForce(sn.getLastMeasuredLatency(), getCoordinates(), sn.getCoordinates());
				updatePredictionError(sn.getLastMeasuredLatency(), getCoordinates(), sn.getCoordinates(), sn.getPredictionError());
				/* Avoid using a sample twice */
				sn.setLastMeasuredLatency(-1);
				

				/* *** Phase3: Update *** */ 
				updateCoordinates(f, sn);
				
			}finally{
				coordinatesLock.writeLock().unlock();		
			}			
			
		}


		/* Phase4: Publish coordinates */
		coordinatesLock.writeLock().lock();
		try{
			publishCoordinates();
			
		}finally{
			coordinatesLock.writeLock().unlock();		
		}			

		
		if(NS_DEBUG){
			System.out.println("[ ] Vivaldi alg, round completed: " + coordinates.toString());
		}

		System.out.println(". Node " + supervisorId + " coordinates: " + coordinates.toString() + " (e. " + predictionError + ")");

	}
	
	
	/* Private functions */
	
	
	/**
	 * Publish Coordinate to ZooKeeper if needed 
	 */
	private void publishCoordinates(){
		
		if (((1 - predictionError) >= CONFIDENCE_THRESHOLD ) && 
				( roundFromLastPublication >= ROUND_BETWEEN_PUBLICATION )){
			/* Last published position is obsolete or not valid anymore: publish coordinates */

			if (NS_DEBUG){
				System.out.println("Publishing coordinates: confidence=" + (1 - predictionError) + " (th. " + CONFIDENCE_THRESHOLD + "), rounds from last publication=" + roundFromLastPublication);
			}

			if (zkClient != null){
				zkClient.setData(getMyZKCoordinateNode(), Serializer.serializeCoordinates(coordinates, predictionError));
			}
			
			roundFromLastPublication = 0;
			
		} else {
			roundFromLastPublication++;
			
			if (NS_DEBUG){
				System.out.println("Coordinates not published: e=" + predictionError + ", round: " + roundFromLastPublication);
			}
		}
		
	}
	
	
	
	/* *************** Vivaldi alg. functions ******************* */
	
	/**
	 * Interrogate remote node (otherNode) to measure latency w.r.t current node
	 * 
	 * @param otherNode
	 * @return
	 */
	private long getLatency(Node otherNode){
		
		if (otherNode == null){
			System.out.println("Invalid otherNode");
			return -1;
		}
		
		String hostname = getHostnameBySupervisorId(otherNode.getSupervisorId());
		
		if (hostname == null){
			System.out.println("Invalid hostname for supervisor-id: " + otherNode.getSupervisorId());
			return -1;
		}
		
		Socket clientSocket;
		long latency = -1;
		
		if (NS_DEBUG){
			System.out.println("Measuring latency to node " + otherNode.getSupervisorId());
		}


		try {
			clientSocket = new Socket(hostname, SERVER_PORT);
			Gson gson = new Gson();
		    
			DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
			DataInputStream ins = new DataInputStream(clientSocket.getInputStream());
			
			/* Step 1 - send request message */
			out.writeByte(NetworkSpaceServer.REQUEST_COORDINATES);
			out.flush();

			/* Step 2 - reading server coordinates */
			long initialTime = System.currentTimeMillis();
			String inputLine = ins.readUTF();
			// Note: is a division between integers 
			latency = (System.currentTimeMillis() - initialTime) / 2;
			
			CoordinateExchangeMessage response = gson.fromJson(inputLine, CoordinateExchangeMessage.class);
			if (response != null && response.getCoordinate() != null){
			    /* Update position for activeSampledNode */
				otherNode.setCoordinates(response.getCoordinate());
				otherNode.setPredictionError(response.getPredictionError());
				otherNode.setLastMeasuredLatency(latency);

				nodesWithKnownPositionLock.lock();
				try{
					nodesWithKnownPosition.put(otherNode.getSupervisorId(), otherNode);
				}finally{
					nodesWithKnownPositionLock.unlock();
				}
			}
			
			/* Step 3 - send my informations (useful to let server compute RTT) */
			CoordinateExchangeMessage message = createCoordinateExchangeMessage();
			out.writeUTF(gson.toJson(message));
			out.flush();
			
			/* Cleanup resources */
			ins.close();
			out.close();
			clientSocket.close();
			
			/* DEBUG: print coordianteExchangeMessage size in byte */
			String serializedMsg = gson.toJson(message);
			byte[] utf8Bytes = serializedMsg.getBytes("UTF-8");
		    int messageSize = utf8Bytes.length;
		    System.out.println(" ++++++++++++++++++++ CoordinateExchangeMessage size: " + messageSize + " bytes ++++++++++++++++++++ ");
			/* DEBUG: print coordianteExchangeMessage size in byte */
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return latency;
	}
	
	
	/**
	 * Get a node from supervisor list with uniform distribution 
	 * 
	 * @return
	 */
	private Node getNode(){
		
		supervisorsLock.lock();
		try{
			int numSupervisors = supervisors.size();
			
			if (numSupervisors == 0)
				return null;
			
			int rndIndex = rnd.nextInt(numSupervisors);
			
			/* Should never happens */
			if (rndIndex == numSupervisors)
				rndIndex--;
			
			SupervisorDetails sv = supervisors.get(rndIndex);
			
			if (sv == null){
				System.out.println("Null node!");
				return null;
			} else {
				Node otherNode = null;
				nodesWithKnownPositionLock.lock();
				try{
					otherNode = nodesWithKnownPosition.get(sv.getId());
				}finally{
					nodesWithKnownPositionLock.unlock();
				}
				
				if (otherNode == null)
					return new Node(networkSpace.getTotalDimensions(), sv);
				else
					return otherNode;
			}
			
		}finally{
			supervisorsLock.unlock();
		}
	}

	/**
	 * Calculate W (see Vivaldi algorithm) 
	 * 
	 * @param otherNodePredictionError
	 * @return
	 */
	private double getW(double otherNodePredictionError){
		
		if (predictionError == 0 && otherNodePredictionError == 0)
			return 1.0;
		else 
			return (predictionError / (predictionError + otherNodePredictionError));
		
	}

	private void updateCoordinates(VivaldiForce f, Node otherNode){
		double w = getW(otherNode.getPredictionError());
		
		double delta = ALPHA * w;

//		System.out.println("ID: " + supervisorId + " - " + coordinates + " (F: " + f);
		
		coordinates = f.movePoint(coordinates, delta);
		
		updateOtherDimensions();
	}
	
	private void updateOtherDimensions(){
		
		if (EXTENDED_SPACE){
			
			if (EXTEND_SPACE_WITH_UTILIZATION){
			
				double cpuusage = getNodeUtilization();
				coordinates.set(networkSpace.getLatencyDimensions(), cpuusage);
				System.out.println("Update usage: " + cpuusage);
				
			} else {

				double reliability = getNodeReliability();
				double rFactor = 1 - reliability;
				coordinates.set(networkSpace.getLatencyDimensions(), rFactor);
				System.out.println("Updated reliability: " + reliability + "(" + rFactor + ")");
				
			}
		}
		
	}
	
	private double updatePredictionError(double latency, Point myCoordinates, Point otherCoordinates, double otherNodePredictionError){
		
		double es = sampleError(latency, myCoordinates, otherCoordinates);
		double w = getW(otherNodePredictionError);
		
		predictionError = es * BETA * w + predictionError * (1 - BETA * w);

//		System.out.println(" [" + supervisorId + "] Prediction error: " + predictionError + "Sample error: " + es + ", w: "+ w +" (l: " + latency + ")");
		return predictionError;

	}
	
	private double sampleError(double latency, Point a, Point b){
		
		double diff = networkSpace.distance(a, b);
		
		if (latency == 0)
			return 0;
		else
			return Math.abs(diff - latency) / Math.abs(latency); 
		
	}

	/* *************** end section (Vivaldi alg. functions) ******************* */

	
	
	private String getHostnameBySupervisorId(String supervisorId){
		if (supervisorId == null)
			return null;
		
		supervisorsLock.lock();
		try{
			for(SupervisorDetails sd: supervisors){
				if (supervisorId.equals(sd.getId()))
					return sd.getHost();
			}
			
			return null;
		}finally{
			supervisorsLock.unlock();
		}
	}

	

	private  CoordinateExchangeMessage createCoordinateExchangeMessage(){
		
		coordinatesLock.readLock().lock();
		try{
			return new CoordinateExchangeMessage(supervisorId, coordinates, predictionError);
		}finally{
			coordinatesLock.readLock().unlock();
		}
		
	}
	
	private String getMyZKCoordinateNode(){
		
		return ZK_COORDINATES_DIR + "/" + supervisorId;
		
	}

	
	
	
	
	/* **** **** **** Getter and setters **** **** **** */
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		
		return this.supervisorId.equals(((Node) obj).getSupervisorId());
	}

	public Point getCoordinates() {
		coordinatesLock.readLock().lock();
		try{
			Point cloned = coordinates.clone();
			return cloned;
		}finally{
			coordinatesLock.readLock().unlock();
		}
	}

	public double getPredictionError() {
		coordinatesLock.readLock().lock();
		try{
			return predictionError;
		}finally{
			coordinatesLock.readLock().unlock();
		}
	}

	public String getSupervisorId() {
		return supervisorId;
	}
	
	public Space getSpace(){
		return networkSpace;
	}
	
	public Map<String, Node> copyKnownNodesCoordinate(){
		Map<String, Node> ret = new HashMap<String, Node>();
		nodesWithKnownPositionLock.lock();
		try{
			for (String sid : nodesWithKnownPosition.keySet()){
				ret.put(sid, nodesWithKnownPosition.get(sid));
			}
			
			/* Add node itself */
			Node me = new Node(coordinates.getDimensionality(), supervisorId);
			me.setCoordinates(coordinates);
			me.setPredictionError(predictionError);
			ret.put(supervisorId, me);
			
			return ret;
		}finally{
			nodesWithKnownPositionLock.unlock();
		}
	}


	public boolean usingExtendedSpace(){
		return EXTENDED_SPACE;
	}

}
