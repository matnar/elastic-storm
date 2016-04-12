package it.uniroma2.adaptivescheduler.space;

import it.uniroma2.adaptivescheduler.entities.Node;

public class KNNItem {
	
	private double distance;
	private Node node;
	
	public KNNItem(double distance, Node node) {
		super();
		this.distance = distance;
		this.node = node;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

	public Node getNode() {
		return node;
	}

	public void setNode(Node node) {
		this.node = node;
	}
	
}
