package it.uniroma2.taos.monitor.stats;

import it.uniroma2.taos.monitor.ipc.MessageHandler;
import it.uniroma2.taos.commons.Logging;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;

public class ExecutorHandler {
	private String executorId;
	private HashMap<String, Integer> tuples = new HashMap<String, Integer>();
	private long lastCputTime = 0;
	private Long curCpu = 0l;
	private long retrieved = System.currentTimeMillis();

	public ExecutorHandler(String executorId) {
		super();
		this.executorId = executorId;
	}

	public ExecutorInfo retrieve() {
		synchronized (curCpu) {

			long monitoredInterval = System.currentTimeMillis() - retrieved;
			float cpuPercent = 0;
			if (lastCputTime == 0) {
				lastCputTime = curCpu;
			} else {
				cpuPercent = (float) (curCpu - lastCputTime) / (float) monitoredInterval;
		//		Logging.append("Cpu delta time received eid: "+executorId+" delta: "+(curCpu - lastCputTime)+ "MonitoredInterval: "+(monitoredInterval*1000000),Level.INFO, ExecutorHandler.class);
				cpuPercent = cpuPercent / 1000000f;
				lastCputTime = curCpu;

			}
			retrieved = System.currentTimeMillis();
			HashMap<String, Integer> mapRet = tuples;
			tuples = new HashMap<String, Integer>();

			return new ExecutorInfo(executorId, mapRet, cpuPercent, monitoredInterval);
		}
	}

	public long getCurCpu() {
		return curCpu;
	}

	public void setCurCpu(long curCpu) {
		synchronized (this.curCpu) {

			this.curCpu = curCpu;
		}
	}

	public void increaseCounter(String src) {
		if (tuples.containsKey(src))
			tuples.put(src, tuples.get(src) + 1);
		else
			tuples.put(src, 1);
	}


	public String getExecutorId() {
		return executorId;
	}


	public HashMap<String, Integer> getTuples() {
		return tuples;
	}

	public long getLastCputTime() {
		return lastCputTime;
	}

	public long getRetrieved() {
		return retrieved;
	}

}
