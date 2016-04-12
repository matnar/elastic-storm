package it.uniroma2.taos.commons.dao;

public class NodeTuple {
	private String nodeId;
	private double currentNodeMhz;
	private int coreCount;
	private double coreMhz;
	private double saturationFactor;

	public NodeTuple(String nodeId, double currentNodeMhz, int coreCount, double coreMhz, double saturationFactor) {
		super();
		this.nodeId = nodeId;
		this.currentNodeMhz = currentNodeMhz;
		this.coreCount = coreCount;
		this.coreMhz = coreMhz;
		this.saturationFactor = saturationFactor;
	}
	@Override
	public boolean equals(Object e) {
		if(!(e instanceof NodeTuple)) return false;
		return this.getNodeId().equals(((NodeTuple)e).getNodeId());
	}

	@Override
	public int hashCode() {
		return getNodeId().hashCode();
	}
	

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public double getCurrentNodeMhz() {
		return currentNodeMhz;
	}

	public void setCurrentNodeMhz(double nodeMhz) {
		this.currentNodeMhz = nodeMhz;
	}

	public int getCoreCount() {
		return coreCount;
	}

	public void setCoreCount(int coreCount) {
		this.coreCount = coreCount;
	}

	public double getCoreMhz() {
		return coreMhz;
	}

	public void setCoreMhz(double coreMhz) {
		this.coreMhz = coreMhz;
	}

	public double getSaturationFactor() {
		return saturationFactor;
	}

	public void setSaturationFactor(double saturationFactor) {
		this.saturationFactor = saturationFactor;
	}

	@Override
	public String toString() {
		return "{nodeId: " + nodeId + " currentNodeMhz: " + currentNodeMhz + " coreCount: " + coreCount + " coreMhz: "
				+ coreMhz + "}";
	}
}
