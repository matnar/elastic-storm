package it.uniroma2.adaptivescheduler.vivaldi;

import it.uniroma2.adaptivescheduler.entities.Node;
import it.uniroma2.adaptivescheduler.space.Point;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;


public class RequestHandler implements Runnable{

	private final Socket clientSocket;
	private final QoSMonitor spaceManager;
	private Gson gson;
	
	private final static boolean DEBUG = false;
	
	public RequestHandler(QoSMonitor spaceManager, Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.spaceManager = spaceManager;
		
		this.gson = new Gson();

		if (DEBUG)
			System.out.println("Handler created.");

	}
	
	@Override
	public void run() {
		
		if (clientSocket == null)
			return;

		if (DEBUG)
			System.out.println("Request received from " + clientSocket.getInetAddress().getHostAddress());
		
		try {
			DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
			DataInputStream ins = new DataInputStream(clientSocket.getInputStream());
			
			String outputMessage;
			String read;
			
			/* Step 1 - receive request message */
			int operationCode = (int) ins.readByte();
			if (DEBUG)
				System.out.println("Operation Code: " + operationCode);
			

			/* Step 2 - send coordinates */
			outputMessage = createAndSerializeMessage();
			out.writeUTF(outputMessage);
			out.flush();

			
			/* Step 3 - receive coordinates */
			long initialTime = System.currentTimeMillis();
			if ((read = ins.readUTF()) != null) {
				
				// Note: is a division between integers
				long latency = (System.currentTimeMillis() - initialTime)/2;
				CoordinateExchangeMessage receivedMessage = deserializeMessage(read);
				
				Node otherNode = receivedMessage.createNode();
				otherNode.setLastMeasuredLatency(latency);
				spaceManager.addLatencyMeasurement(otherNode);
				
				if (DEBUG){
					System.out.println("Received Coordinates: " + receivedMessage.getCoordinate() + 
							" (e:" + receivedMessage.getPredictionError() + ", latency: " + latency + ")");
				}
			}
			
			
			/* Cleanup resources */
			ins.close();
			out.close();
			clientSocket.close();
		    
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	
	
	private String createAndSerializeMessage(){
		
		Point coordinates = spaceManager.getCoordinates();
		double predictionError = spaceManager.getPredictionError();
		String supervisorId = spaceManager.getSupervisorId();
		
		CoordinateExchangeMessage message = new CoordinateExchangeMessage(supervisorId, coordinates, predictionError);
		
		return gson.toJson(message);
		
	}
	
	private CoordinateExchangeMessage deserializeMessage(String message){
	
		try{
			return gson.fromJson(message, CoordinateExchangeMessage.class);
		}catch(JsonParseException parse){
			parse.printStackTrace();
			return null;
		}
		
	}

}
