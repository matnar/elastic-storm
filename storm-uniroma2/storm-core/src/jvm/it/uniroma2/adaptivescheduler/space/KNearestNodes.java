package it.uniroma2.adaptivescheduler.space;

import it.uniroma2.adaptivescheduler.entities.Node;

import java.util.List;
import java.util.Map;

public interface KNearestNodes {

	public List<KNNItem> getKNearestNode(int k, Point position, Map<String, Node> nodes);
	
}
