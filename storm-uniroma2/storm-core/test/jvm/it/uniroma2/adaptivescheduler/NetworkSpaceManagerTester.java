package it.uniroma2.adaptivescheduler;

import it.uniroma2.adaptivescheduler.vivaldi.QoSMonitor;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import backtype.storm.scheduler.SupervisorDetails;

public class NetworkSpaceManagerTester {

	private QoSMonitor manager; 
	
	public NetworkSpaceManagerTester() {
		manager = new QoSMonitor("test-id");
	}
	

	@Test
	public void executeRound() {
		
		List<SupervisorDetails> supervisors = new ArrayList<SupervisorDetails>();
		
		supervisors.add(new SupervisorDetails("my-host", "127.0.0.1", null, null));
		
		manager.updateNodes(supervisors);
		manager.executeSingleRound();
		
	}

	@Test
	public void initialize() {
		
		manager.initialize();
		
	}

	
}
