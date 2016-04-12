package it.uniroma2.adaptivescheduler;

import org.junit.Test;

public class WorkerMonitorTester {

	@Test
	public void test() throws ClassNotFoundException {
		
		WorkerMonitor wm = new WorkerMonitor("stormid", "assignmentId", "workerId", 10);
		
		wm.initialize();
		
	}

}
