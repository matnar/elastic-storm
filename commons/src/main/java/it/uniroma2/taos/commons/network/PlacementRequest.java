package it.uniroma2.taos.commons.network;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class PlacementRequest {
    // placement degli executor per i quali si richiede scheduling
    private ArrayList<ExecutorTopo> executors = new ArrayList<ExecutorTopo>();
    // lista dei nodi sui quali si vuole eseguire lo scheluding
    public ArrayList<NodeSlots> availableNodes = new ArrayList<NodeSlots>();
    // lista delle topologie che sono sogette a rebalance
    public ArrayList<String> reabalanceTopoIds = new ArrayList<String>();

    public ArrayList<String> getReabalanceTopoIds() {
	return reabalanceTopoIds;
    }

    public void setReabalanceTopoIds(ArrayList<String> reabalanceTopoIds) {
	this.reabalanceTopoIds = reabalanceTopoIds;
    }

    public ArrayList<ExecutorTopo> getExecutors() {
	return executors;
    }

    public void setExecutors(ArrayList<ExecutorTopo> executors) {
	this.executors = executors;
    }

    public ArrayList<NodeSlots> getAvailableNodes() {
	return availableNodes;
    }

    public void setAvailableNodes(ArrayList<NodeSlots> availableNodes) {
	this.availableNodes = availableNodes;
    }

    public PlacementRequest() {
	// TODO Auto-generated constructor stub
    }

    public PlacementRequest(ArrayList<ExecutorTopo> executors, ArrayList<NodeSlots> availableNodes) {
	super();
	this.executors = executors;
	this.availableNodes = availableNodes;
    }

    public String toString() {
	return executors.toString() + " " + availableNodes.toString();
    }

    public ArrayList<ExecutorTopo> executorsByTopo(String topoId) {
	ArrayList<ExecutorTopo> toReturn = new ArrayList<ExecutorTopo>();
	for (ExecutorTopo exectopo : executors) {
	    if (exectopo.getTopoId().equals(topoId))
	    toReturn.add(exectopo);
	}
	return toReturn;
    }

}
