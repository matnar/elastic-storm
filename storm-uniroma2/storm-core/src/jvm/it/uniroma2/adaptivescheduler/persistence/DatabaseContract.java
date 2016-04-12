package it.uniroma2.adaptivescheduler.persistence;

public class DatabaseContract {
	
	public static final String TABLE_MEASUREMENTS = "MEASUREMENTS";

	public class MeasurementsTable{

		/* Fields */
		public static final String ID 			= "id";
		public static final String TASK_FROM 	= "task_from";
		public static final String TASK_TO 		= "task_to";
		public static final String VALUE 		= "value";
		public static final String STORM_ID 	= "storm_id";
		public static final String ASSIGNMENT_ID= "assignment_id";
		public static final String WORKER_ID 	= "worker_id";
		public static final String PORT 		= "worker_port";
		
	
		/* Management Queries */
		public static final String SQL_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "+ TABLE_MEASUREMENTS +" "
				+ "("+ ID + " IDENTITY, "+ TASK_FROM + " VARCHAR(255), "
				+ TASK_TO + " VARCHAR(255), " + VALUE + " DOUBLE , "
				+ STORM_ID + " VARCHAR(255), " + ASSIGNMENT_ID + " VARCHAR(255), " 
				+ WORKER_ID + " VARCHAR(255), " + PORT + " INT )";
		public static final String SQL_DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE_MEASUREMENTS;
		
	}
	

}
