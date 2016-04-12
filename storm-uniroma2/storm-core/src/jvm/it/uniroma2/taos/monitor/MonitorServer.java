package it.uniroma2.taos.monitor;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.io.FileUtils;

import backtype.storm.Config;

import com.mongodb.client.model.UpdateOptions;

import it.uniroma2.taos.monitor.ipc.ExecutorMessageHandler;
import it.uniroma2.taos.monitor.stats.ExecutorInfo;
import it.uniroma2.taos.commons.Constants;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.Utils;
import it.uniroma2.taos.commons.dao.ExecutorTuple;
import it.uniroma2.taos.commons.dao.NodeTuple;
import it.uniroma2.taos.commons.dao.TrafficTuple;
import it.uniroma2.taos.commons.persistence.MongoManager;

public class MonitorServer {
	
	private static int CPU_CORE_COUNT = 1; // default value, real value will be
											// retrieved via linux shell command
	private static double CPU_CORE_MHZ = 1000; // default value, real value will
												// be retrieved via linux shell
												// command
	private String nodeId;
	final String basePath = System.getProperty("java.io.tmpdir") + File.separator + "storm" + File.separator
			+ "Chronicle";
	private static double SATFAC=1.0;
	public MonitorServer(String nodeId) {

		Logging.append("Starting Monitor Server for nodeId: " + nodeId, Level.INFO, MonitorServer.class);
		// try {
		// FileUtils.cleanDirectory(new File(basePath));
		// } catch (IOException e) {
		// Logging.append(e.getMessage(),Level.INFO,this.getClass());
		// }
		
		this.nodeId = nodeId;
		Logging.append("Creating Mongo Manager and Executor Message Handler for " + nodeId, Level.INFO,
				MonitorServer.class);
		Map confs=backtype.storm.utils.Utils.readStormConfig();
		SATFAC = (Double) confs.get(Config.TAOS_TARGET_UTILIZATION);
		String mongoIp = (String) confs.get(Config.TAOS_MONGO_DB_IP);
		Integer mongoPort = (Integer) confs.get(Config.TAOS_MONGO_DB_PORT);
		int ttl = (Integer) confs.get(Config.TAOS_DB_TTL);
		Double updateDamping = (Double) confs.get(Config.TAOS_METRICS_SMOOTHING);
		final MongoManager manager = new MongoManager(mongoIp, mongoPort, ttl,updateDamping.floatValue());
		final ExecutorMessageHandler mh = new ExecutorMessageHandler();
		final long fetch=((Integer) confs.get(Config.TAOS_METRICS_UPDATE_PERIOD)).longValue();
		Logging.append("Created Mongo Manager and Executor Message Handler for " + nodeId, Level.INFO,
				MonitorServer.class);
		String[] cmd = { "/bin/sh", "-c", "cat /proc/cpuinfo | grep MHz" };
		String tmp = Utils.executeCommand(cmd);
		tmp = tmp.replace(" ", "");// removes white space
		tmp = tmp.replace("	", "");// removes tab
		tmp = tmp.split("\n")[0];
		CPU_CORE_MHZ = Double.parseDouble(tmp.split(":")[1]);
		cmd[2] = "cat /proc/cpuinfo | grep cores";
		tmp = Utils.executeCommand(cmd);
		tmp = tmp.replace(" ", "");// removes white space
		tmp = tmp.replace("	", "");// removes tab
		tmp = tmp.split("\n")[0];
		CPU_CORE_COUNT = Integer.parseInt(tmp.split(":")[1]);
		Thread t = new Thread(mh);
		t.start();
		t = new Thread(new Runnable() {

			public void run() {
				while (true) {
					ArrayList<ExecutorInfo> datas = mh.retrieveDataStats();
					submitStats(datas, manager);
					
					try {
						Thread.sleep(fetch);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						Logging.append(e.getMessage(), Level.SEVERE, this.getClass());
					}
				}

			}
		});
		t.start();

		Logging.append("Initialization complete, Monitor Server for nodeId: " + nodeId, Level.INFO, MonitorServer.class);

	}

	public void submitStats(ArrayList<ExecutorInfo> eiList, MongoManager manager) {
		double totalMhz = 0;
		for (ExecutorInfo executorInfo : eiList) {
			double executorMhz = (executorInfo.getCpuPercent() * CPU_CORE_MHZ);
			totalMhz += executorMhz;

			String executorId = executorInfo.getExecutorId();
			manager.upsertExecutor(new ExecutorTuple(executorId, executorMhz,null));

			for (Entry<String, Integer> e : executorInfo.getTuples().entrySet()) {
				
				manager.upsertTraffic(new TrafficTuple(null,e.getKey(), executorId, e.getValue()));
			}
		}
		if (nodeId.equals(""))
			return;
		manager.upsertNode(new NodeTuple(nodeId, totalMhz, CPU_CORE_COUNT, CPU_CORE_MHZ,SATFAC));

	}
}
