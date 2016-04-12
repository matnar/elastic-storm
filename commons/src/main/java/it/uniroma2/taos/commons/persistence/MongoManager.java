package it.uniroma2.taos.commons.persistence;

import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.dao.ExecutorTuple;
import it.uniroma2.taos.commons.dao.NodeTuple;
import it.uniroma2.taos.commons.dao.TrafficTuple;
import it.uniroma2.taos.commons.network.PlacementResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.mongodb.MongoClient;

public class MongoManager {

    private static MongoClient mongo = null;
    private static Database db = null;
    private float dampingConstant = 0.5f;
    private static final Logger logger = LogManager.getLogger(MongoManager.class.getName());

    public MongoManager(String ip, int port, int db_ttl, float damping_constant) {
	if (mongo == null) {
	    mongo = new MongoClient(ip, port);
	}
	if (db == null) {
	    db = new Database(mongo, "StormStats", db_ttl);
	}
	this.dampingConstant = damping_constant;

    }

    public MongoManager(String ip, int port) {
	if (mongo == null) {
	    mongo = new MongoClient(ip, port);
	}
	if (db == null) {
	    db = new Database(mongo, "StormStats");
	}

    }
    public void closeClient()
    {
	mongo.close();
    }
    public ArrayList<ExecutorTuple> getExecutor(String executorId) {
	ArrayList<ExecutorTuple> toReturn = new ArrayList<ExecutorTuple>();
	Document query = new Document();
	if (executorId != null)
	    query.put("execId", executorId);
	for (Document d : db.getEc().find(query)) {
	    toReturn.add(new ExecutorTuple(d.getString("execId"), d.getDouble("Mhz"), d.getDate("date")));
	}
	return toReturn;
    }

    public NodeTuple getNode(String nodeId) {
	if (nodeId == null)
	    return null;
	Document query = new Document();
	query.put("nodeId", nodeId);
	double Mhz = -1;
	int cores = 0;
	double maxMhz = 0;
	double factor = 1;
	for (Document d : db.getNc().find(query)) {
	    Mhz = d.getDouble("Mhz");
	    cores = d.getInteger("cores");
	    maxMhz = d.getDouble("maxMhz");
	    factor = d.getDouble("satfac");
	}
	if (Mhz == -1)
	    return null;
	return new NodeTuple(nodeId, Mhz, cores, maxMhz, factor);
    }

    public ArrayList<NodeTuple> getNodes(String nodeId) {
	ArrayList<NodeTuple> toReturn = new ArrayList<NodeTuple>();
	Document query = new Document();
	if (nodeId != null)
	    query.put("nodeId", nodeId);
	double Mhz = -1;
	int cores = 0;
	double maxMhz = 0;
	double factor = 1;
	for (Document d : db.getNc().find(query)) {
	    Mhz = d.getDouble("Mhz");
	    cores = d.getInteger("cores");
	    maxMhz = d.getDouble("maxMhz");
	    factor = d.getDouble("satfac");
	    toReturn.add(new NodeTuple(d.getString("nodeId"), Mhz, cores, maxMhz, factor));
	}
	return toReturn;
    }

    public ArrayList<NodeTuple> getNode() {
	ArrayList<NodeTuple> toReturn = new ArrayList<NodeTuple>();
	Document query = new Document();
	double Mhz = -1;
	int cores = 0;
	double maxMhz = 0;
	double factor = 1;
	String nodeId = null;
	for (Document d : db.getNc().find(query)) {
	    Mhz = d.getDouble("Mhz");
	    cores = d.getInteger("cores");
	    maxMhz = d.getDouble("maxMhz");
	    factor = d.getDouble("satfac");
	    nodeId = d.getString("nodeId");
	    toReturn.add(new NodeTuple(nodeId, Mhz, cores, maxMhz, factor));
	}
	return toReturn;
    }

    public ArrayList<TrafficTuple> getTraffic(String sourceId, String destId) {
	if (sourceId == null && destId == null)
	    return null;
	if (sourceId == null) {
	    ArrayList<TrafficTuple> toReturn = new ArrayList<TrafficTuple>();
	    Document query = new Document();
	    query.put("destination", destId);
	    for (Document d : db.getTr().find(query)) {

		int count = d.getInteger("count");
		toReturn.add(new TrafficTuple(d.getDate("date"), d.getString("source"), destId, count));
	    }
	    return toReturn;

	} else if (destId == null) {
	    ArrayList<TrafficTuple> toReturn = new ArrayList<TrafficTuple>();
	    Document query = new Document();
	    query.put("source", sourceId);
	    for (Document d : db.getTr().find(query)) {

		int count = d.getInteger("count");
		toReturn.add(new TrafficTuple(d.getDate("date"), sourceId, d.getString("destination"), count));
	    }
	    return toReturn;
	} else {
	    ArrayList<TrafficTuple> toReturn = new ArrayList<TrafficTuple>();
	    Document query = new Document();
	    query.put("source", sourceId);
	    query.put("destination", destId);
	    for (Document d : db.getTr().find(query)) {
		int count = d.getInteger("count");
		toReturn.add(new TrafficTuple(d.getDate("date"), sourceId, destId, count));
	    }
	    return toReturn;

	}
    }

    public void upsertTraffic(TrafficTuple tt) {

	Document query = new Document();
	query.put("source", tt.getSource());
	query.put("destination", tt.getDestination());
	int oldcount = -1;
	for (Document d : db.getTr().find(query)) {
	    oldcount = d.getInteger("count");
	}
	if (oldcount == -1) {
	    Document newDoc = new Document();
	    newDoc.put("source", tt.getSource());
	    newDoc.put("destination", tt.getDestination());
	    newDoc.put("count", tt.getCount());
	    newDoc.put("date", new Date());
	    Logging.append("Submitting traffic stat " + newDoc.toJson(), Level.INFO, MongoManager.class);
	    db.getTr().insertOne(newDoc);
	}

	else {
	    float dampedNewCount = dampingConstant * ((float) tt.getCount());
	    float dampedOldCount = (1f - dampingConstant) * ((float) oldcount);
	    int newCount = (int) (dampedNewCount + dampedOldCount);
	    Document newDoc = new Document();
	    newDoc.put("source", tt.getSource());
	    newDoc.put("destination", tt.getDestination());
	    newDoc.put("count", newCount);
	    newDoc.put("date", new Date());
	    Logging.append("Updating traffic stat " + newDoc.toJson(), Level.INFO, MongoManager.class);
	    db.getTr().updateOne(query, new Document("$set", newDoc));
	}
    }

    public void upsertExecutor(ExecutorTuple et) {
	Document execQuery = new Document();
	execQuery.put("execId", et.getExecutorId());
	double oldMhz = -1;
	for (Document d : db.getEc().find(execQuery)) {
	    oldMhz = d.getDouble("Mhz");
	}
	if (oldMhz == -1) {
	    Document newDoc = new Document();
	    newDoc.put("execId", et.getExecutorId());
	    newDoc.put("Mhz", et.getExecutorMhz());
	    newDoc.put("date", new Date());
	    Logging.append("Submitting executor cpu stat " + newDoc.toJson(), Level.INFO, MongoManager.class);
	    db.getEc().insertOne(newDoc);

	} else {
	    double newMhz = oldMhz * (1 - dampingConstant) + et.getExecutorMhz() * dampingConstant;
	    Document newDoc = new Document();
	    newDoc.put("execId", et.getExecutorId());
	    newDoc.put("Mhz", newMhz);
	    newDoc.put("date", new Date());
	    Logging.append("updating executor cpu stat " + newDoc.toJson(), Level.INFO, MongoManager.class);
	    db.getEc().updateOne(execQuery, new Document("$set", newDoc));

	}
    }

    public void upsertNode(NodeTuple nt) {
	Document nodeQuery = new Document();
	nodeQuery.put("nodeId", nt.getNodeId());
	double oldMhz = -1;
	for (Document d : db.getNc().find(nodeQuery)) {
	    oldMhz = d.getDouble("Mhz");
	}
	if (oldMhz == -1) {
	    Document newDoc = new Document();
	    newDoc.put("nodeId", nt.getNodeId());
	    newDoc.put("Mhz", nt.getCurrentNodeMhz());
	    newDoc.put("maxMhz", nt.getCoreMhz());
	    newDoc.put("cores", nt.getCoreCount());
	    newDoc.put("satfac", nt.getSaturationFactor());
	    newDoc.put("date", new Date());
	    Logging.append("Submitting node cpu stat " + newDoc.toJson(), Level.INFO, MongoManager.class);
	    db.getNc().insertOne(newDoc);

	} else {
	    double newMhz = (oldMhz * (1 - dampingConstant) + nt.getCurrentNodeMhz() * dampingConstant);
	    Document newDoc = new Document();
	    newDoc.put("nodeId", nt.getNodeId());
	    newDoc.put("Mhz", newMhz);
	    newDoc.put("maxMhz", nt.getCoreMhz());
	    newDoc.put("cores", nt.getCoreCount());
	    newDoc.put("satfac", nt.getSaturationFactor());
	    newDoc.put("date", new Date());
	    Logging.append("updating node cpu stat " + newDoc.toJson(), Level.INFO, MongoManager.class);
	    db.getNc().updateOne(nodeQuery, new Document("$set", newDoc));
	}

    }

    public PlacementResponse getDepl() {
	Document deplQuery = new Document();
	for (Document d : db.getTo().find(deplQuery)) {
	    String jsonDepl = d.getString("deployment");
	    ObjectMapper mapper = new ObjectMapper();
	    try {
		return mapper.readValue(jsonDepl, PlacementResponse.class);
	    } catch (JsonParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (JsonMappingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
	return null;

    }

    public void delExecutors(ExecutorTuple et) {
	Document execQuery = new Document();
	execQuery.put("execId", et.getExecutorId());
	double oldMhz = -1;
	for (Document d : db.getEc().find(execQuery)) {
	    oldMhz = d.getDouble("Mhz");
	}
	if (oldMhz != -1) {
	    db.getEc().deleteMany(execQuery);
	}
    }

    public void delTraffic(TrafficTuple tt) {
	Document query = new Document();
	if (tt.getSource() != null)
	    query.put("source", tt.getSource());
	if (tt.getDestination() != null)
	    query.put("destination", tt.getDestination());
	int oldcount = -1;
	for (Document d : db.getTr().find(query)) {
	    oldcount = d.getInteger("count");
	}
	if (oldcount != -1) {
	    db.getTr().deleteMany(query);
	}

    }

    public void delNode(NodeTuple nt) {
	Document query = new Document();
	if (nt.getNodeId() != null)
	    query.put("nodeId", nt.getNodeId());
	int cores = -1;
	for (Document d : db.getNc().find(query)) {
	    cores = d.getInteger("cores");
	}
	if (cores != -1) {
	    db.getNc().deleteMany(query);
	}
    }

    public void upsertDepl(PlacementResponse pr) {
	ObjectMapper mapper = new ObjectMapper();
	String jsonDeployment = null;
	try {
	    jsonDeployment = mapper.writeValueAsString(pr);

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
	Document deplQuery = new Document();
	Document newDoc = new Document();
	newDoc.put("deployment", jsonDeployment);
	newDoc.put("date", new Date());

	if (!db.getTo().find(deplQuery).iterator().hasNext()) {
	    db.getTo().insertOne(newDoc);
	} else {
	    db.getTo().updateOne(deplQuery, new Document("$set", newDoc));
	}

    }

    public void putNe(int executorNumber) {
	Document newDoc = new Document();
	newDoc.put("executors", executorNumber);
	newDoc.put("date", System.currentTimeMillis());
	db.getNe().insertOne(newDoc);
    }

    public void putMigTime(long msecs) {
	Document newDoc = new Document();
	newDoc.put("time", msecs);
	newDoc.put("date", System.currentTimeMillis());
	db.getMt().insertOne(newDoc);
    }

    public void putLatency(long msecs) {
	Document newDoc = new Document();
	newDoc.put("latency", msecs);
	newDoc.put("date", System.currentTimeMillis());
	db.getRt().insertOne(newDoc);
    }

    public void putTempSize(long size, String id) {
	Document newDoc = new Document();
	newDoc.put("id", id);
	newDoc.put("size", size);
	newDoc.put("date", System.currentTimeMillis());
	db.getTsm().insertOne(newDoc);
    }

    public void delTempMigSize() {
	Document query = new Document();
	db.getTsm().deleteMany(query);

    }

    public long getTempMigSize() {
	Document query = new Document();
	long toReturn = 0;
	for (Document d : db.getTsm().find(query)) {
	    toReturn += d.getLong("size");
	}
	return toReturn;
    }

    public void putTempTupleCount(long size, String id) {
	Document newDoc = new Document();
	newDoc.put("id", id);
	newDoc.put("size", size);
	newDoc.put("date", System.currentTimeMillis());
	db.getTtc().insertOne(newDoc);
    }

    public void delTempTupleCount() {
	Document query = new Document();
	db.getTtc().deleteMany(query);

    }

    public long getTempTupleCount() {
	Document query = new Document();
	long toReturn = 0;
	for (Document d : db.getTtc().find(query)) {
	    toReturn += d.getLong("size");
	}
	return toReturn;
    }

    public void putTupleCount(long size) {
	Document newDoc = new Document();
	newDoc.put("size", size);
	newDoc.put("date", System.currentTimeMillis());
	db.getTc().insertOne(newDoc);
    }

    public void delTupleCount() {
	Document query = new Document();
	db.getTc().deleteMany(query);

    }

    public void putTotalMigSize(long size) {
	Document newDoc = new Document();
	newDoc.put("size", size);
	newDoc.put("date", System.currentTimeMillis());
	db.getSm().insertOne(newDoc);
    }

    public void delTotalMigSize() {
	Document query = new Document();
	db.getSm().deleteMany(query);

    }

    public void delNe() {
	Document query = new Document();
	db.getNe().deleteMany(query);
    }

    public void delLat() {
	Document query = new Document();
	db.getRt().deleteMany(query);
    }

    public void delMigTime() {
	Document query = new Document();
	db.getMt().deleteMany(query);
    }

    public void delNetwork() {
	Document query = new Document();
	db.getNt().deleteMany(query);
    }

    public void putNetworkCost(long cost) {
	Document newDoc = new Document();
	newDoc.put("cost", cost);
	newDoc.put("date", System.currentTimeMillis());
	db.getNt().insertOne(newDoc);
    }
    public void putTrh(long trh) {
	Document newDoc = new Document();
	newDoc.put("cost", trh);
	newDoc.put("date", System.currentTimeMillis());
	db.getTrh().insertOne(newDoc);
    }

    public void putUtilization(double min, double max, double avg) {
	Document newDoc = new Document();
	newDoc.put("date", System.currentTimeMillis());
	newDoc.put("min", min);
	newDoc.put("max", max);
	newDoc.put("avg", avg);
	db.getUt().insertOne(newDoc);
    }

    public void delUtilization() {
	Document query = new Document();
	db.getUt().deleteMany(query);
    }

    public void delMDetectors() {
	Document query = new Document();
	db.getMDetectors().deleteMany(query);
    }
    public void putMDetectors(int num) {
	Document newDoc = new Document();
	newDoc.put("date", System.currentTimeMillis());
	newDoc.put("num", num);
	db.getMDetectors().insertOne(newDoc);
    }
    
    public void putMovDetectors(int num)
    {
	Document newDoc = new Document();
	newDoc.put("date", System.currentTimeMillis());
	newDoc.put("num", num);
	db.getMovDetectors().insertOne(newDoc);
    }
    public void delMovDetectors()
    {
	Document query = new Document();
	db.getMovDetectors().deleteMany(query);
    }
    public void putMovDetectorsTmp()
    {
	Document newDoc = new Document();
	newDoc.put("date", System.currentTimeMillis());
	db.getMovDetectorsTmp().insertOne(newDoc);
    }
    public void delMovDetectorsTmp()
    {
	Document query = new Document();
	db.getMovDetectorsTmp().deleteMany(query);
    }
    public int getMovDetectorsCount()
    {
	Document query = new Document();
	int toreturn=0;
	for (Document d : db.getMovDetectorsTmp().find(query)) {
	    toreturn++;
	}
	return toreturn;
    }
    public void delRdyTasks()
    {
	Document query=new Document();
	db.getRdyTasks().deleteMany(query);
    }
    public void putRdyTask(int task)
    {
	Document newDoc = new Document();
	newDoc.put("task", task);
	db.getRdyTasks().insertOne(newDoc);
    }
    public ArrayList<Integer> getRdyTasks()
    {
	ArrayList<Integer> toReturn=new ArrayList<Integer>();
	Document newDoc = new Document();
	for (Document d : db.getRdyTasks().find(newDoc)) {
	    toReturn.add(d.getInteger("task"));
	}
	return toReturn;
	
    }

}
