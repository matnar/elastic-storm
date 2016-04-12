package it.uniroma2.adaptivescheduler.scheduler.internal;

import backtype.storm.scheduler.WorkerSlot;

public class AugmentedWorkerSlot implements Comparable<AugmentedWorkerSlot> {

	private int port;
	private String hostname;
	private WorkerSlot workerSlot;

	
	public AugmentedWorkerSlot(int port, String hostname, WorkerSlot workerSlot) {
		super();
		this.port = port;
		this.hostname = hostname;
		this.workerSlot = workerSlot;
	}

	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public WorkerSlot getWorkerSlot() {
		return workerSlot;
	}
	public void setWorkerSlot(WorkerSlot workerSlot) {
		this.workerSlot = workerSlot;
	}


	@Override
	public int compareTo(AugmentedWorkerSlot o) {
		
		if (this.hostname == null || o == null || o.hostname == null ||  this.hostname.compareToIgnoreCase(o.hostname) < 0)
			return -1;
		else if (this.hostname.compareToIgnoreCase(o.hostname) > 0)
			return 1;
		else{
			if (this.port <= o.port)
				return -1;
			else 
				return 1;
		}
	}
	
}
