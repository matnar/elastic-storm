package it.uniroma2.adaptivescheduler.scheduler.internal;

import java.util.List;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class AugmentedExecutorDetails {
	
	private ExecutorDetails executor;
	private String componentId; 
	private List<String> sourceComponentsId; 
	private List<String> targetComponentsId; 
	private WorkerSlot workerSlot;
	
	
	public AugmentedExecutorDetails(ExecutorDetails executor, String componentId) {
		super();
		this.executor = executor;
		this.componentId = componentId;
	}
	
	
	public ExecutorDetails getExecutor() {
		return executor;
	}
	public void setExecutor(ExecutorDetails executor) {
		this.executor = executor;
	}
	public String getComponentId() {
		return componentId;
	}
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	public List<String> getSourceComponentsId() {
		return sourceComponentsId;
	}
	public void setSourceComponentsId(List<String> sourceComponentsId) {
		this.sourceComponentsId = sourceComponentsId;
	}
	public List<String> getTargetComponentsId() {
		return targetComponentsId;
	}
	public void setTargetComponentsId(List<String> targetComponentsId) {
		this.targetComponentsId = targetComponentsId;
	}
	public WorkerSlot getWorkerSlot() {
		return workerSlot;
	}
	public void setWorkerSlot(WorkerSlot workerSlot) {
		this.workerSlot = workerSlot;
	}
	
	

}
