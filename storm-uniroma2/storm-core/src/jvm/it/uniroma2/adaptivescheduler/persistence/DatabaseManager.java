package it.uniroma2.adaptivescheduler.persistence;

import it.uniroma2.adaptivescheduler.persistence.DatabaseContract.MeasurementsTable;
import it.uniroma2.adaptivescheduler.persistence.entities.Measurement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.Server;

public class DatabaseManager {

	/* Security is not considered by current DB Manager */
	private static final String DB_USER = "uniroma2";
	private static final String DB_PSW = "";
	
	private JdbcConnectionPool connectionPool; 
	private String supervisorId = null;
	private String baseDir = "./";
	
	private Server server = null; 
	
	private int port = -1;
	
	public DatabaseManager(String baseDir, String supervisorId) throws ClassNotFoundException {
	
		this.supervisorId = supervisorId;		
		this.baseDir = baseDir;
	}
	
	private String getConnectionUrl(int port) throws DatabaseException{

		if (supervisorId ==  null){
			throw new DatabaseException("Invalid supervisor id");
		}
		
		String connectionURL = "jdbc:h2:";
		
		if (port != -1)
			connectionURL += "tcp://localhost:" + port + "/";
		
		return connectionURL + baseDir + supervisorId;
		
	}
	
	public void startServer(int port) throws SQLException{
		
		this.port = port;
		server = Server.createTcpServer("-tcpPort", Integer.toString(port), "-tcpDaemon", "-baseDir", baseDir).start();
		
	}
	
	public void stopServer(){
		
		if (server != null)
			server.stop();
		
	}

	public void initializeOnSupervisor() throws ClassNotFoundException, DatabaseException, SQLException{

		if (port < 0)
			throw new DatabaseException("Invalid database connection address: port " + port + " has been indicated");
		
		/* Create the connection pool */
		connectionPool = JdbcConnectionPool.create(getConnectionUrl(port), DB_USER, DB_PSW);

		cleanup();

	}
	
	public void initialize(int port) throws ClassNotFoundException, DatabaseException, SQLException{

		if (port < 0)
			throw new DatabaseException("Invalid database connection address: port " + port + " has been indicated");
		
		/* Create the connection pool */
		connectionPool = JdbcConnectionPool.create(getConnectionUrl(port), DB_USER, DB_PSW);

		/* Create tables */
		createTablesIfNotExists();
		
	}
	
	
	public void cleanup(){

		// XXX: this operation can be implemented in a more efficient way
		
		try {
			deleteTables();
			createTablesIfNotExists();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public void dispose(){
	
		try {
			deleteTables();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		connectionPool.dispose();

		connectionPool = null;
	}
	

	private void createTablesIfNotExists() throws SQLException{
		Connection conn = connectionPool.getConnection();
		Statement s = conn.createStatement();
		
		s.execute(DatabaseContract.MeasurementsTable.SQL_CREATE_TABLE);
		
		/* Closing connection */
		s.close();
		conn.close();
		
	}
	
	
	
	private void deleteTables() throws SQLException {
		
		Connection conn = connectionPool.getConnection();
		Statement s = conn.createStatement();
		
		s.execute(DatabaseContract.MeasurementsTable.SQL_DROP_TABLE);
		
		/* Closing connection */
		s.close();
		conn.close();
		
	}
	
	
	public boolean saveMeasurement(Measurement measure, boolean override) throws SQLException, DatabaseException{
		
		if (measure == null)
			throw new DatabaseException("Invalid measurement");
		
		int inserted = 0;

		if (override){
			
			Measurement m = getMeasurement(measure.getTaskFrom(), measure.getTaskTo(), measure.getWorkerId(), measure.getPort());

			/* Check if measurement already exists */
			if (m != null){

				measure.setId(m.getId());
				
				boolean updated = updateMeasurement(measure);
				
				if (updated)
					return updated; 
			}
		} 

		Connection conn = connectionPool.getConnection();

		/* Insert measurement */
		PreparedStatement ps = conn.prepareStatement("INSERT INTO " + DatabaseContract.TABLE_MEASUREMENTS + " ("
				+ MeasurementsTable.TASK_FROM +", " + MeasurementsTable.TASK_TO +", " + MeasurementsTable.VALUE + ", "
				+ MeasurementsTable.STORM_ID+", " + MeasurementsTable.ASSIGNMENT_ID+", " + MeasurementsTable.WORKER_ID + ", "
				+ MeasurementsTable.PORT +") VALUES (?, ?, ?, ?, ?, ?, ?)");
			
		ps.setString(1, measure.getTaskFrom());
		ps.setString(2, measure.getTaskTo());
		ps.setDouble(3, measure.getValue());
		ps.setString(4, measure.getStormId());
		ps.setString(5, measure.getAssignmentId());
		ps.setString(6, measure.getWorkerId());
		ps.setInt(7, measure.getPort());
		
		inserted = ps.executeUpdate();
		ps.close();
		conn.close();
		

		return (inserted > 0);
	}

	
	private boolean updateMeasurement(Measurement measure) throws SQLException, DatabaseException{
		
		if (measure == null)
			throw new DatabaseException("Invalid measurement");
		
		int updated = 0;

		Connection conn = connectionPool.getConnection();
		
		PreparedStatement ps = conn.prepareStatement("UPDATE " + DatabaseContract.TABLE_MEASUREMENTS + " SET "
				+ MeasurementsTable.TASK_FROM +"=?, " + MeasurementsTable.TASK_TO +"=?, " 
				+ MeasurementsTable.VALUE + "=?, " + MeasurementsTable.STORM_ID+"=?, " 
				+ MeasurementsTable.ASSIGNMENT_ID+"=?, " + MeasurementsTable.WORKER_ID + "=?, "
				+ MeasurementsTable.PORT +"=? "
				+ " WHERE " + MeasurementsTable.ID + "=?;");
		
		ps.setString(1, measure.getTaskFrom());
		ps.setString(2, measure.getTaskTo());
		ps.setDouble(3, measure.getValue());
		ps.setString(4, measure.getStormId());
		ps.setString(5, measure.getAssignmentId());
		ps.setString(6, measure.getWorkerId());
		ps.setInt(7, measure.getPort());
		
		ps.setLong(8, measure.getId());
		
		updated = ps.executeUpdate();

		ps.close();
		conn.close();
	

		return (updated > 0);
	}
	
	private Measurement getMeasurement(String taskFrom, String taskTo, String workerId, int workerPort) throws SQLException{
		
		Measurement m = null;

		Connection conn = connectionPool.getConnection();
				
		PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + DatabaseContract.TABLE_MEASUREMENTS + 
				" WHERE "
					+ MeasurementsTable.TASK_FROM +"=?	AND " 
					+ MeasurementsTable.TASK_TO +"=? 	AND " 
					+ MeasurementsTable.WORKER_ID + "=? AND "
					+ MeasurementsTable.PORT +"=? ");
			
			
		ps.setString(1, taskFrom);
		ps.setString(2, taskTo);
		ps.setString(3, workerId);
		ps.setInt(4, workerPort);
			
		ResultSet rs = ps.executeQuery();
		
		if (rs.next()){
			Long id = rs.getLong(MeasurementsTable.ID);
			String from = rs.getString(MeasurementsTable.TASK_FROM);
			String to = rs.getString(MeasurementsTable.TASK_TO);
			Double val = rs.getDouble(MeasurementsTable.VALUE);
			String dbStormId = rs.getString(MeasurementsTable.STORM_ID);
			String dbAssignmentId = rs.getString(MeasurementsTable.ASSIGNMENT_ID);
			String dbWorkerId = rs.getString(MeasurementsTable.WORKER_ID);
			Integer dbworkerPort = rs.getInt(MeasurementsTable.PORT);
		
			m = new Measurement(id, from, to, val, dbStormId, dbAssignmentId, dbWorkerId, dbworkerPort);
		}
		rs.close();
		ps.close();
		conn.close();
		
		return m;
	}
	
	public List<Measurement> getAllMeasurements() throws SQLException, DatabaseException{
		
		
		Connection conn = connectionPool.getConnection();
		PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + DatabaseContract.TABLE_MEASUREMENTS);

		ResultSet rs = ps.executeQuery();
		List<Measurement> measurements = new ArrayList<Measurement>();
		
		while (rs.next()){
			Long id = rs.getLong(MeasurementsTable.ID);
			String from = rs.getString(MeasurementsTable.TASK_FROM);
			String to = rs.getString(MeasurementsTable.TASK_TO);
			Double val = rs.getDouble(MeasurementsTable.VALUE);
			String stormId = rs.getString(MeasurementsTable.STORM_ID);
			String assignmentId = rs.getString(MeasurementsTable.ASSIGNMENT_ID);
			String workerId = rs.getString(MeasurementsTable.WORKER_ID);
			int workerPort = rs.getInt(MeasurementsTable.PORT);
			
			Measurement m = new Measurement(id, from, to, val, stormId, assignmentId, workerId, workerPort);
			measurements.add(m);
		}
		
		rs.close();
		ps.close();
		conn.close();

		return measurements;
	}
	
	public List<Measurement> getMeasurements(String taskFrom, String taskTo, String topologyId) throws SQLException, DatabaseException{
		
		
		Connection conn = connectionPool.getConnection();
		PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + DatabaseContract.TABLE_MEASUREMENTS
				+ " WHERE " + MeasurementsTable.TASK_FROM + "=? AND "
				+ MeasurementsTable.TASK_TO +"=? AND " 
				+ MeasurementsTable.STORM_ID +"=? ; ");

		ps.setString(1, taskFrom);
		ps.setString(2, taskTo);
		ps.setString(3, topologyId);
		
		ResultSet rs = ps.executeQuery();
		List<Measurement> measurements = new ArrayList<Measurement>();
		
		while (rs.next()){
			Long id = rs.getLong(MeasurementsTable.ID);
			String from = rs.getString(MeasurementsTable.TASK_FROM);
			String to = rs.getString(MeasurementsTable.TASK_TO);
			Double val = rs.getDouble(MeasurementsTable.VALUE);
			String stormId = rs.getString(MeasurementsTable.STORM_ID);
			String assignmentId = rs.getString(MeasurementsTable.ASSIGNMENT_ID);
			String workerId = rs.getString(MeasurementsTable.WORKER_ID);
			int workerPort = rs.getInt(MeasurementsTable.PORT);
			
			Measurement m = new Measurement(id, from, to, val, stormId, assignmentId, workerId, workerPort);
			measurements.add(m);
		}
		
		rs.close();
		ps.close();
		conn.close();

		return measurements;
	}

	
	public int deleteMeasurements(String task, String topologyId) throws SQLException{
	
		Connection conn = connectionPool.getConnection();
		PreparedStatement ps = conn.prepareStatement("DELETE FROM " + DatabaseContract.TABLE_MEASUREMENTS
				+ " WHERE ( " + MeasurementsTable.TASK_FROM + "=? OR "
				+ MeasurementsTable.TASK_TO +"=? ) AND " 
				+ MeasurementsTable.STORM_ID +"=? ; ");

		ps.setString(1, task);
		ps.setString(2, task);
		ps.setString(3, topologyId);
		
		int deleted = ps.executeUpdate();
		
		ps.close();
		conn.close();

		return deleted;
			
	}
	
	
}
