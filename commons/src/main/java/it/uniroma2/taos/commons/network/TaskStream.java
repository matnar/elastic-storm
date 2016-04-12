package it.uniroma2.taos.commons.network;

public class TaskStream {
	private int taskId;
	private String stream;
	public TaskStream(){};
	public TaskStream(int taskId, String stream) {
		super();
		this.taskId = taskId;
		this.stream = stream;
	}
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public String getStream() {
		return stream;
	}
	public void setStream(String stream) {
		this.stream = stream;
	}
	@Override
	public boolean equals(Object o)
	{
		if(taskId==((TaskStream)o).getTaskId() && stream.equals(((TaskStream)o).getStream()))return true;
		return false;
	}
	public String toString()
	{
	    return  taskId+"@"+stream;
	}

}
