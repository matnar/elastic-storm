package it.uniroma2.taos.commons.dao;

import java.util.Date;

public class ExecutorTuple {

	private String executorId;
	private double executorMhz;
	private Date date;

	public ExecutorTuple(String executorId, double executorMhz, Date date) {
		super();
		this.date = date;
		this.executorId = executorId;
		this.executorMhz = executorMhz;
	}

	public String getExecutorId() {
		return executorId;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public void setExecutorId(String executorId) {
		this.executorId = executorId;
	}

	public double getExecutorMhz() {
		return executorMhz;
	}

	public void setExecutorMhz(double executorMhz) {
		this.executorMhz = executorMhz;
	}
	@Override
	public boolean equals(Object e) {
		if(!(e instanceof ExecutorTuple)) return false;
		return this.getExecutorId().equals(((ExecutorTuple)e).getExecutorId());
	}

	@Override
	public int hashCode() {
		return executorId.hashCode();
	}

}
