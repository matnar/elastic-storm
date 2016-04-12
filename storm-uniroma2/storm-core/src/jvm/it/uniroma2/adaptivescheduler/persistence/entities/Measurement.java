package it.uniroma2.adaptivescheduler.persistence.entities;

public class Measurement {

	private Long id;
	private String taskFrom;
	private String taskTo;
	private double value;
	
	private String stormId; 
	private String assignmentId; 
	private String workerId; 
	private int port; 

	
	
	public Measurement(Long id, String taskFrom, String taskTo, double value,
			String stormId, String assignmentId, String workerId, int port) {
		super();
		this.id = id;
		this.taskFrom = taskFrom;
		this.taskTo = taskTo;
		this.value = value;
		this.stormId = stormId;
		this.assignmentId = assignmentId;
		this.workerId = workerId;
		this.port = port;
	}

	
	
	public Measurement(String taskFrom, String taskTo, double value,
			String stormId, String assignmentId, String workerId, int port) {
		super();
		this.id = null;
		this.taskFrom = taskFrom;
		this.taskTo = taskTo;
		this.value = value;
		this.stormId = stormId;
		this.assignmentId = assignmentId;
		this.workerId = workerId;
		this.port = port;
	}



	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getTaskFrom() {
		return taskFrom;
	}

	public void setTaskFrom(String taskFrom) {
		this.taskFrom = taskFrom;
	}

	public String getTaskTo() {
		return taskTo;
	}

	public void setTaskTo(String taskTo) {
		this.taskTo = taskTo;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
	
	public String getStormId() {
		return stormId;
	}



	public void setStormId(String stormId) {
		this.stormId = stormId;
	}



	public String getAssignmentId() {
		return assignmentId;
	}



	public void setAssignmentId(String assignmentId) {
		this.assignmentId = assignmentId;
	}



	public String getWorkerId() {
		return workerId;
	}



	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}



	public int getPort() {
		return port;
	}



	public void setPort(int port) {
		this.port = port;
	}



	@Override
	public String toString() {
		return stormId + " [" + taskFrom + "," + taskTo + "] " + value;
	}
}
