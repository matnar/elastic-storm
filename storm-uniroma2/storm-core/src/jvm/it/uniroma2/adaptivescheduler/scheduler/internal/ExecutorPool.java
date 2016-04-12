package it.uniroma2.adaptivescheduler.scheduler.internal;

import it.uniroma2.adaptivescheduler.entities.Node;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class ExecutorPool {
	
	private boolean pinned; 
	
	/* Indicate if current executor pool should be rescheduled or not */
	private boolean assigned; 
	
	private Set<ExecutorDetails> executors;
	private Set<String> components;
	
	private Node position; 
	private WorkerSlot workerSlot;
	
	private boolean containsSources;
	private boolean containsTargets;
	
	
	private List<ExecutorPool> parentExecutorPools;
	private Set<String> parentComponents;
	private List<ExecutorPool> childExecutorPools;
	private Set<String> childComponents;
	
	
	public ExecutorPool() {
		super();
		
		position = null;
		workerSlot =  null;
		
		pinned = false;
		assigned = false;
		
		containsSources = false;
		containsTargets = false;
		
		executors = new HashSet<ExecutorDetails>();
		components = new HashSet<String>();
		
		parentExecutorPools = new ArrayList<ExecutorPool>();
		childExecutorPools = new ArrayList<ExecutorPool>();
		parentComponents = new HashSet<String>();
		childComponents = new HashSet<String>();
		
	}


	public boolean isPinned() {
		return pinned;
	}


	public void setPinned(boolean pinned) {
		this.pinned = pinned;
	}


	public boolean isAssigned() {
		return assigned;
	}


	public void setAssigned(boolean assigned) {
		this.assigned = assigned;
	}


	public Set<ExecutorDetails> getExecutors() {
		return executors;
	}


	public void setExecutors(Set<ExecutorDetails> executors) {
		this.executors = executors;
	}


	public Set<String> getComponents() {
		return components;
	}


	public void setComponents(Set<String> components) {
		this.components = components;
	}


	public Node getPosition() {
		return position;
	}


	public void setPosition(Node position) {
		this.position = position;
	}


	public WorkerSlot getWorkerSlot() {
		return workerSlot;
	}


	public void setWorkerSlot(WorkerSlot workerSlot) {
		this.workerSlot = workerSlot;
	}


	public List<ExecutorPool> getParentExecutorPools() {
		return parentExecutorPools;
	}


	public void setParentExecutorPools(List<ExecutorPool> parentExecutorPools) {
		this.parentExecutorPools = parentExecutorPools;
	}


	public Set<String> getParentComponents() {
		return parentComponents;
	}


	public void setParentComponents(Set<String> parentComponents) {
		this.parentComponents = parentComponents;
	}


	public List<ExecutorPool> getChildExecutorPools() {
		return childExecutorPools;
	}


	public void setChildExecutorPools(List<ExecutorPool> childExecutorPools) {
		this.childExecutorPools = childExecutorPools;
	}


	public Set<String> getChildComponents() {
		return childComponents;
	}


	public void setChildComponents(Set<String> childComponents) {
		this.childComponents = childComponents;
	}


	public boolean isContainsSources() {
		return containsSources;
	}


	public void setContainsSources(boolean containsSources) {
		this.containsSources = containsSources;
	}


	public boolean isContainsTargets() {
		return containsTargets;
	}


	public void setContainsTargets(boolean containsTargets) {
		this.containsTargets = containsTargets;
	}
	
	@Override
	public String toString() {
		return executors.toString();
	}
}
