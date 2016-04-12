package it.uniroma2.adaptivescheduler;

import org.junit.Test;

public class AdaptationManagerTester {

	@Test
	public void initialize() {
		
		AdaptationManager mgr = new AdaptationManager();
		
		mgr.initialize();
	
		
	}
	
	@Test
	public void executeScheduler() {
		
		AdaptationManager mgr = new AdaptationManager();
		
		mgr.initialize();
		
		mgr.executeContinuousScheduler(null, null, null);
	
		
	}
	
	

}
