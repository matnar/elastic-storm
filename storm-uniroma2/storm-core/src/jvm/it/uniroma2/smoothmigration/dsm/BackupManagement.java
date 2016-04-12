package it.uniroma2.smoothmigration.dsm;

import it.uniroma2.smoothmigration.bolt.SmoothBolt;
import it.uniroma2.smoothmigration.spout.SmoothSpout;
import it.uniroma2.taos.commons.Logging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.type.TypeFactory;

import ch.qos.logback.classic.Logger;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.Tuple;

public class BackupManagement {
    private KryoTupleSerializer kts = null;
    private KryoTupleDeserializer ktd = null;
    private KryoValuesSerializer kvs = null;
    private KryoValuesDeserializer kvd = null;
    private HazelcastInstance localhostClient = null;
    private HazelcastInstance remoteClient = null;
    private String taskId;
    private String remoteClientAddr = null;
    private static final String BUFFER_MAP_NAME = "BufferBackups";
    private static final String STATE_MAP_NAME = "StateBackups";
    private static double STATE_TIMEOUT=5000;

    // private ObjectMapper mapper = new ObjectMapper();

    public BackupManagement(String remoteClientAddr, String taskId, GeneralTopologyContext gtc, SmoothBolt sb) {
	this.remoteClientAddr = remoteClientAddr;
	this.taskId = taskId;
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	if (kts == null) {
	    kts = new KryoTupleSerializer(confs, gtc);
	    ktd = new KryoTupleDeserializer(confs, gtc);
	    kvs = new KryoValuesSerializer(confs);
	    kvd = new KryoValuesDeserializer(confs);
	}
	// mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	ClientConfig clientConfig = new ClientConfig();
	clientConfig.addAddress("localhost:5701");
	if (sb.getSerializers() != null) {
	    for (SerializerConfig sc : sb.getSerializers()) {
		clientConfig.getSerializationConfig().addSerializerConfig(sc);
	    }
	}
	localhostClient = HazelcastClient.newHazelcastClient(clientConfig);
	if (remoteClientAddr != null) {
	    clientConfig = new ClientConfig();
	    if (sb.getSerializers() != null) {
		for (SerializerConfig sc : sb.getSerializers()) {
		    clientConfig.getSerializationConfig().addSerializerConfig(sc);
		}
	    }

	    clientConfig.addAddress(remoteClientAddr + ":5701");
	    this.remoteClient = HazelcastClient.newHazelcastClient(clientConfig);
	}
    }

    public BackupManagement(String remoteClientAddr, String taskId, GeneralTopologyContext gtc, SmoothSpout sp) {
	this.taskId = taskId;
	Map confs = backtype.storm.utils.Utils.readStormConfig();
	if (kts == null) {
	    kts = new KryoTupleSerializer(confs, gtc);
	    ktd = new KryoTupleDeserializer(confs, gtc);
	    kvs = new KryoValuesSerializer(confs);
	    kvd = new KryoValuesDeserializer(confs);
	}
	// mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
	ClientConfig clientConfig = new ClientConfig();
	clientConfig.addAddress("localhost:5701");
	if (sp.getSerializers() != null) {
	    for (SerializerConfig sc : sp.getSerializers()) {
		clientConfig.getSerializationConfig().addSerializerConfig(sc);
	    }
	}
	localhostClient = HazelcastClient.newHazelcastClient(clientConfig);
	if (remoteClientAddr != null) {
	    clientConfig = new ClientConfig();
	    clientConfig.addAddress(remoteClientAddr + ":5701");
	    if (sp.getSerializers() != null) {
		for (SerializerConfig sc : sp.getSerializers()) {
		    clientConfig.getSerializationConfig().addSerializerConfig(sc);
		}
	    }
	    this.remoteClient = HazelcastClient.newHazelcastClient(clientConfig);
	}
    }

    public void backupTuples(ArrayList<Tuple> buffer) {
	Logging.append("started tuple backup " + taskId, Level.INFO, this.getClass());

	IMap<String, ArrayList<byte[]>> backupMap = localhostClient.getMap(BUFFER_MAP_NAME);
	ArrayList<byte[]> toPut = new ArrayList<byte[]>();
	for (Tuple tuple : buffer) {
	    toPut.add(kts.serialize(tuple));
	}
	while (true) {
	    backupMap.set(taskId, toPut);
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    IMap<String, ArrayList<byte[]>> testMap = localhostClient.getMap(BUFFER_MAP_NAME);
	    if (!testMap.containsKey(taskId)) {
		Logging.append("fail tuple backup " + taskId, Level.INFO, this.getClass());
		continue;
	    }

	    break;
	}
	Logging.append("Writed tuples " + taskId, Level.INFO, this.getClass());

    }

    public ArrayList<Tuple> restoreTuples(boolean mustExist) {

	long start=System.currentTimeMillis();
	if (remoteClient == null)
	    return new ArrayList<Tuple>();
	IMap<String, ArrayList<byte[]>> backupMap = remoteClient.getMap(BUFFER_MAP_NAME);
	ArrayList<byte[]> toRestoreByte = null;
	while (true) {
	    Logging.append("waiting to appear tuples, keys are: " + backupMap.keySet().toString(), Level.INFO, this.getClass());
	    toRestoreByte = backupMap.get(taskId);
	    if (toRestoreByte == null) {
		backupMap = remoteClient.getMap(BUFFER_MAP_NAME);
	    } else
		break;
	    if (!mustExist || System.currentTimeMillis()-start>STATE_TIMEOUT)
		break;
	    try {
		Thread.sleep(100);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}
	ArrayList<Tuple> toRestore = new ArrayList<Tuple>();
	if (toRestoreByte == null)
	    return toRestore;

	for (byte[] bs : toRestoreByte) {
	    toRestore.add(ktd.deserialize(bs));
	}
	backupMap.remove(taskId);
	return toRestore;
    }

    public void backupState(HashMap<String, Object> toBackup) {
	Logging.append("Starting state backup", Level.INFO, this.getClass());
	
	while (true) {
	    IMap<String, HashMap<String, Object>> backupMap = localhostClient.getMap(STATE_MAP_NAME);

	    backupMap.set(taskId, toBackup);
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	    IMap<String, HashMap<String, Object>> testMap = localhostClient.getMap(STATE_MAP_NAME);
	    if (!testMap.containsKey(taskId)) {
		Logging.append("fail state backup " + taskId, Level.INFO, this.getClass());
		continue;
	    }

	    break;
	}
	Logging.append("Writed state", Level.INFO, this.getClass());

    }

    public HashMap<String, Object> restoreState(boolean mustExist) {
	Logging.append("Starting state restore", Level.INFO, this.getClass());
	long start=System.currentTimeMillis();
	if (remoteClient == null)
	    return null;
	HashMap<String, Object> state = null;
	IMap<String, HashMap<String, Object>> backupMap = remoteClient.getMap(STATE_MAP_NAME);
	while (true) {
	    Logging.append("ip: " + remoteClientAddr + " taskid: " + taskId + " waiting to appear, keys are: " + backupMap.keySet().toString(),
		    Level.INFO, this.getClass());
	    state = backupMap.get(taskId);
	    if (state == null)
		backupMap = remoteClient.getMap(STATE_MAP_NAME);
	    else
		break;
	    if (!mustExist || System.currentTimeMillis()-start>STATE_TIMEOUT)
		break;
	    try {
		Thread.sleep(100);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
	if (state == null)
	    return null;
	Logging.append("Completed state backup", Level.INFO, this.getClass());
	backupMap.remove(taskId);
	return state;
    }

}
