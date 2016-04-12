package it.uniroma2.taos.commons.network;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class Assignment {
	private String nodeId;
	private String topoId;
	private ArrayList<String> executorsIds;
	private int port=-1;

	public Assignment(String nodeId, String topoId, ArrayList<String> executorsIds) {
		super();
		this.nodeId = nodeId;
		this.topoId = topoId;
		this.executorsIds = executorsIds;
	}

	public Assignment() {
	};

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getTopoId() {
		return topoId;
	}

	public void setTopoId(String topoId) {
		this.topoId = topoId;
	}

	public ArrayList<String> getExecutorsIds() {
		return executorsIds;
	}

	public void setExecutorsIds(ArrayList<String> executorsIds) {
		this.executorsIds = executorsIds;
	}

	@Override
	public boolean equals(Object e) {
		if (!nodeId.equals(((Assignment) e).getNodeId()) || !topoId.equals(((Assignment) e).getTopoId()) || executorsIds.size()!=((Assignment)e).getExecutorsIds().size()) return false;
		for (String string : executorsIds) {
			if(!((Assignment)e).getExecutorsIds().contains(string)) return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		Collections.sort(executorsIds);
		String concat=nodeId+topoId;
		for (String string : executorsIds) {
			concat+=executorsIds;
		}
		return  concat.hashCode();
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	public String toString()
	{
		return "nodeId: "+ nodeId+" topoId: " + topoId+" executorsIds: "+executorsIds.toString();
	}

}
