package it.uniroma2.adaptivescheduler.vivaldi;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class NetworkSpaceServer implements Runnable{

	public static final int REQUEST_COORDINATES = 100;
	
	private final int port; 
	private final QoSMonitor spaceManager; 
	private ServerSocket serverSocket;
	
	public NetworkSpaceServer(int serverPort, QoSMonitor spaceManager) {
		this.port = serverPort;
		this.spaceManager = spaceManager;
		
	}
	
	@Override
	public void run(){
		try {
			serverSocket = new ServerSocket(port);
			Socket clientSocket;

			System.out.println("Starting Network Space Server on "
					+ serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getLocalPort() + " ... ");
			
			while((clientSocket = serverSocket.accept()) != null){
				Thread handler = new Thread(new RequestHandler(spaceManager, clientSocket));
				handler.start();
			}
			
		} catch (SocketException to){
			// DEBUG: 
			System.out.println("Socket closed, terminating server...");
		
		} catch (IOException e) {
			System.out.println("errore: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("errore: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void terminate(){
		try {
			if (serverSocket != null && !serverSocket.isClosed()){
				serverSocket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
