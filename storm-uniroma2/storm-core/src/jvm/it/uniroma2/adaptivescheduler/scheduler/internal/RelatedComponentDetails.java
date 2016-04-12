package it.uniroma2.adaptivescheduler.scheduler.internal;

import it.uniroma2.adaptivescheduler.entities.Node;

import java.util.List;
import java.util.Map;

import backtype.storm.scheduler.WorkerSlot;

public class RelatedComponentDetails {

	public static enum Type {
		PARENT, 
		CHILD
	}
	
	private String componentId; 
	private Map<String, Node> networkSpaceCoordinates = null; 
	private List<Integer> task = null;
	private List<WorkerSlot> workerSlots = null;
	private Type type;
	
	
	public RelatedComponentDetails(String componentId, Type type) {
		super();
		this.componentId = componentId;
		this.type = type;
	}
	
	
	public String getComponentId() {
		return componentId;
	}
	public void setComponentId(String componentId) {
		this.componentId = componentId;
	}
	public Map<String, Node> getNetworkSpaceCoordinates() {
		return networkSpaceCoordinates;
	}
	public void setNetworkSpaceCoordinates(Map<String, Node> networkSpaceCoordinates) {
		this.networkSpaceCoordinates = networkSpaceCoordinates;
	}
	public List<Integer> getTask() {
		return task;
	}
	public void setTask(List<Integer> task) {
		this.task = task;
	}
	public List<WorkerSlot> getWorkerSlots() {
		return workerSlots;
	}
	public void setWorkerSlots(List<WorkerSlot> workerSlots) {
		this.workerSlots = workerSlots;
	}
	public Type getType() {
		return type;
	}
	public void setType(Type type) {
		this.type = type;
	}
	
	
	
}
