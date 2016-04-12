package it.uniroma2.taos.commons.network;

public class TaskTopo {
    boolean crashed = false;

    private int taskId;
    private String topoId;

    public TaskTopo() {
    };

    public TaskTopo(int taskId, String topoId) {
	super();
	this.taskId = taskId;
	this.topoId = topoId;
    }

    public int getTaskId() {
	return taskId;
    }

    public void setTaskId(int taskId) {
	this.taskId = taskId;
    }

    public String getTopoId() {
	return topoId;
    }

    public void setTopoId(String topoId) {
	this.topoId = topoId;
    }

    @Override
    public boolean equals(Object o) {
	if (o == null)
	    return false;
	if (taskId == ((TaskTopo) o).getTaskId() && topoId.equals(((TaskTopo) o).getTopoId()))
	    return true;
	return false;
    }

    public String toString() {
	return taskId + "@" + topoId;
    }

    public boolean isCrashed() {
	return crashed;
    }

    public void setCrashed(boolean crashed) {
	this.crashed = crashed;
    }

}
