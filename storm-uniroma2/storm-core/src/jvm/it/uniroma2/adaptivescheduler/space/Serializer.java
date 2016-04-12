package it.uniroma2.adaptivescheduler.space;

import it.uniroma2.adaptivescheduler.entities.Node;

public class Serializer {
	private static final String SEP_COORDINATES_DIMENSIONS = ";";
	private static final String SEP_COORDINATES_ITEMS = "::";

	public static byte[] serializeCoordinates(Point coordinates, double predictionError){

		String output = "";
		
		int dimensionality = coordinates.getDimensionality();
		
		/* Serialize coordinates */
		for(int i = 0; i < dimensionality; i++){
			
			output += coordinates.get(i);
			
			if (i != (dimensionality - 1)){
				output += SEP_COORDINATES_DIMENSIONS;
			}
		}
		
		output+= SEP_COORDINATES_ITEMS;
		
		output+= predictionError;

		return output.getBytes();
	}
	
	
	public static Node deserializeCoordinates(String nodeId, byte[] data){

		/* Coordinates are serialized as: 
		 * 	X;Y;Z::ERROR
		 */
		String serialized = new String(data);
		
		String[] items = serialized.split(SEP_COORDINATES_ITEMS);
		
		String[] coordinates = items[0].split(SEP_COORDINATES_DIMENSIONS);
		int dimensionality = coordinates.length;
		
		Point p = new Point(dimensionality);
		for(int i = 0; i < dimensionality; i++){
			p.set(i, Double.parseDouble(coordinates[i]));
		}
		
		double error = Double.parseDouble(items[1]);

		Node n = new Node(dimensionality, nodeId);
		n.setCoordinates(p);
		n.setPredictionError(error);
		
		return n;
	}

}
