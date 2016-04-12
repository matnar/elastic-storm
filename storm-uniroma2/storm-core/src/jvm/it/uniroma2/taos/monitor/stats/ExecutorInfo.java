package it.uniroma2.taos.monitor.stats;

import java.util.HashMap;

public class ExecutorInfo {

	private String executorId;
	private HashMap<String,Integer> tuples=new HashMap<String, Integer>();
	private float cpuPercent;
	private long monitoredInteval;
	public ExecutorInfo(String executorId,
			HashMap<String, Integer> tuples, float cpuPercent,
			long monitoredInteval) {
		super();
		this.executorId = executorId;
		this.tuples = tuples;
		this.cpuPercent = cpuPercent;
		this.monitoredInteval = monitoredInteval;
	}
	public String getExecutorId() {
		return executorId;
	}

	public HashMap<String, Integer> getTuples() {
		return tuples;
	}
	public float getCpuPercent() {
		return cpuPercent;
	}
	public long getMonitoredInteval() {
		return monitoredInteval;
	}
	
	
}
