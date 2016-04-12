package it.uniroma2.taos.commons.network;

public class NodeSlots {
	String nodeId;
	//numero di slot disponibili sul nodo
	int availableSlots;
	
	public NodeSlots(String nodeId, int availableSlots) {
		super();
		this.nodeId = nodeId;
		this.availableSlots = availableSlots;
	}
	public NodeSlots() {
		// TODO Auto-generated constructor stub
	}
	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public int getAvailableSlots() {
		return availableSlots;
	}
	public void setAvailableSlots(int availableSlots) {
		this.availableSlots = availableSlots;
	}
	public String toString()
	{
		return nodeId+" : "+availableSlots; 
	}
	
	@Override
	public boolean equals(Object e)
	{
		if(this.availableSlots==((NodeSlots)e).getAvailableSlots() && nodeId.equals(((NodeSlots)e).getNodeId())) return true; 
		return false;
	}
	@Override
	public int hashCode() {;
		return this.toString().hashCode();
	}
	

}
