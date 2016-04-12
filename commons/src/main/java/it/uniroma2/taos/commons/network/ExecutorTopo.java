package it.uniroma2.taos.commons.network;

import org.codehaus.jackson.annotate.JsonIgnore;

public class ExecutorTopo {
	String topoId;
	String exeId;
	public ExecutorTopo() {
		// TODO Auto-generated constructor stub
	}
	public ExecutorTopo(String topoId, String exeId) {
		super();
		this.topoId = topoId;
		this.exeId = exeId;
	}
	@JsonIgnore
	public Integer getLowerIndex()
	{
	    String tmp=exeId.substring(1, exeId.length()-1);
	    String[] splits=tmp.split(" ");
	    return Integer.parseInt(splits[0]);
	}
	@JsonIgnore
	public Integer getUpperIndex()
	{
	    String tmp=exeId.substring(1, exeId.length()-1);
	    String[] splits=tmp.split(" ");
	    return Integer.parseInt(splits[1]);
	}
	public String getTopoId() {
		return topoId;
	}
	public void setTopoId(String topoId) {
		this.topoId = topoId;
	}
	public String getExeId() {
		return exeId;
	}
	public void setExeId(String exeId) {
		this.exeId = exeId;
	}
	@Override
	public boolean equals(Object e)
	{
		if(((ExecutorTopo)e).getMongoId().equals(this.getMongoId())) return true;
		return false;
	}
	@Override
	public int hashCode() {;
		return getMongoId().hashCode();
	}
	@JsonIgnore
	public String getMongoId()
	{
		return exeId+"@"+topoId;
	}
	public String toString()
	{
		return getMongoId();
	}

}
