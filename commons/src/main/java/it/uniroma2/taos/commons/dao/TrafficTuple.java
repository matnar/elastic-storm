package it.uniroma2.taos.commons.dao;

import java.util.Date;

public class TrafficTuple {
	private String source;
	private String destination;
	private int count;
	private Date date;



	public TrafficTuple(Date date,String source, String destination, int count) {
		super();
		this.source = source;
		this.destination = destination;
		this.count = count;
		this.date=date;
	}
	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}
	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	public String toString()
	{
		return "{source: "+source+" destination: "+destination+" count: "+count+"}";
	}
	@Override
	public boolean equals(Object e) {
		if(!(e instanceof TrafficTuple)) return false;
		return (this.getSource().equals(((TrafficTuple)e).getSource()) && this.getDestination().equals(((TrafficTuple)e).getDestination()));
	}

	@Override
	public int hashCode() {
		String concat=this.getSource()+this.getDestination();
		return concat.hashCode();
	}

}
