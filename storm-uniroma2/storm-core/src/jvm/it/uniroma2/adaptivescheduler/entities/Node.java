package it.uniroma2.adaptivescheduler.entities;

import backtype.storm.scheduler.SupervisorDetails;
import it.uniroma2.adaptivescheduler.space.Point;

public class Node {
	
	private String supervisorId;
	private Point coordinates;
	private double predictionError;
    private long lastMeasuredLatency; /* it's expressed in millisec */

    private long lastUpdate; 
    
	public Node(int dimensionality, String supervisorId) {
		coordinates = new Point(dimensionality);
		predictionError = 1.0;
		this.supervisorId = supervisorId;
		lastMeasuredLatency = -1;
		lastUpdate = System.currentTimeMillis();
	}

	public Node(int dimensionality, SupervisorDetails supervisor) {
		coordinates = new Point(dimensionality);
		predictionError = 1.0;
		if (supervisor != null)
			this.supervisorId = supervisor.getId();
		else 
			this.supervisorId = null;
		lastMeasuredLatency = -1;
	}

	public String getSupervisorId() {
		return supervisorId;
	}

	public void setSupervisorId(String supervisorId) {
		this.supervisorId = supervisorId;
	}

	public Point getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(Point coordinates) {
		this.coordinates = coordinates;
		lastUpdate = System.currentTimeMillis();
	}

	public double getPredictionError() {
		return predictionError;
	}

	public void setPredictionError(double predictionError) {
		this.predictionError = predictionError;
	}

	public long getLastMeasuredLatency() {
		return lastMeasuredLatency;
	}

	public void setLastMeasuredLatency(long lastMeasuredLatency) {
		this.lastMeasuredLatency = lastMeasuredLatency;
	}
	
	
	public long getLastUpdateMillis() {
		return lastUpdate;
	}

	@Override
	public boolean equals(Object obj) {
		
		if (obj instanceof Node){
			Node n = (Node) obj;
			
			if (supervisorId != null){
				return supervisorId.equals(n.supervisorId);
			}else{
				return (n.supervisorId == null);
			}
			
		}
		
		return super.equals(obj);
	}
	
	@Override
	public String toString() {
		String repr = this.supervisorId + " [" + this.coordinates.toString() + "] e." + this.predictionError; 
		return repr;
	}
}
