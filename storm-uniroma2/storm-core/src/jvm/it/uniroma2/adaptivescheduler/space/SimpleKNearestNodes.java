package it.uniroma2.adaptivescheduler.space;

import it.uniroma2.adaptivescheduler.entities.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleKNearestNodes implements KNearestNodes{

	Space space = null;
	
	public SimpleKNearestNodes(Space space) {
		this.space = space;
	}
	
	@Override
	public List<KNNItem> getKNearestNode(int k, Point position,
			Map<String, Node> nodes) {

		List<KNNItem> nearest = new ArrayList<KNNItem>();
		List<String> blacklist = new ArrayList<String>();
		if (nodes == null)
			return nearest;
		
		int nearestFound = 0;
		int maxNNodes = nodes.values().size();
		k = Math.min(k, maxNNodes);

		boolean mixedSpace = false;
		if (space instanceof BimodalSpace){
			mixedSpace = true;
		}
		
		while(nearestFound < k){
			
			Node currentNearest = null;
			double currentMinDistance = Double.MAX_VALUE;
			
			for (Node n : nodes.values()){
				double dist = Double.MAX_VALUE; 
				
				if (mixedSpace){
					dist = ((BimodalSpace) space).distance(position, n.getCoordinates(), true);
				}else
					dist = space.distance(position, n.getCoordinates());

				if (dist < currentMinDistance && !blacklist.contains(n.getSupervisorId())){
					/* Update nearest node */

					currentNearest = n;
					currentMinDistance = dist; 
				}
			}
			
			if (currentNearest != null){
				KNNItem item = new KNNItem(currentMinDistance, currentNearest);
				nearest.add(item);
				nearestFound++;
				
				blacklist.add(currentNearest.getSupervisorId());
			} else {
				break;
			}
		}
		
		
		return nearest;
	}

}
