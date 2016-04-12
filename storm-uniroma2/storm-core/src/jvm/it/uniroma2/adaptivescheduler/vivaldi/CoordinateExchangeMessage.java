package it.uniroma2.adaptivescheduler.vivaldi;

import it.uniroma2.adaptivescheduler.entities.Node;
import it.uniroma2.adaptivescheduler.space.Point;

public class CoordinateExchangeMessage {

	private Point coordinate; 
	private double predictionError;
	private String supervisorId;
	
	public CoordinateExchangeMessage(String supervisorId, Point coordinate, double predictionError) {
		this.coordinate = coordinate;
		this.predictionError = predictionError;
		this.supervisorId = supervisorId;
	}


	public Point getCoordinate() {
		return coordinate;
	}


	public void setCoordinate(Point coordinate) {
		this.coordinate = coordinate;
	}


	public double getPredictionError() {
		return predictionError;
	}


	public void setPredictionError(double predictionError) {
		this.predictionError = predictionError;
	}


	public String getSupervisorId() {
		return supervisorId;
	}


	public void setSupervisorId(String supervisorId) {
		this.supervisorId = supervisorId;
	}


	public Node createNode(){

		if (coordinate != null){
			Node n = new Node(coordinate.getDimensionality(), supervisorId);
			n.setPredictionError(predictionError);
			n.setCoordinates(coordinate);
			
			return n;
			
		} else {
			return null;
		}
				
	}
	
}
