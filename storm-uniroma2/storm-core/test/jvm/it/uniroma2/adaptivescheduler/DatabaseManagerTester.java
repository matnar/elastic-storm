package it.uniroma2.adaptivescheduler;

import it.uniroma2.adaptivescheduler.persistence.DatabaseException;
import it.uniroma2.adaptivescheduler.persistence.DatabaseManager;
import it.uniroma2.adaptivescheduler.persistence.entities.Measurement;

import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

public class DatabaseManagerTester {

	private static String supervisorId = "test-db";
	
	
	@Test
	public void connection() throws ClassNotFoundException, DatabaseException, SQLException {
		
		DatabaseManager mgr = new DatabaseManager("./", supervisorId);
		
		mgr.initialize(-1);
	}
	
	@Test
	public void addMeasure() throws ClassNotFoundException, DatabaseException, SQLException{
		
		DatabaseManager mgr = new DatabaseManager("./", supervisorId);
		
		mgr.initialize(-1);

		Measurement measure = new Measurement("a", "b", 20.2, "stormid", "assignmentid", "workerid", 6700);
		mgr.saveMeasurement(measure, true);
	}

	
	@Test
	public void getAllMeasurements() throws ClassNotFoundException, DatabaseException, SQLException{
		
		DatabaseManager mgr = new DatabaseManager("./", supervisorId);
		
		mgr.initialize(-1);

		List<Measurement> ms = mgr.getAllMeasurements();
		
		for (Measurement m : ms){
			System.out.println(m);
		}
	}
	
}
