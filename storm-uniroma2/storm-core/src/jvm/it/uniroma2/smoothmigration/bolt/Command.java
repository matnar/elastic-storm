package it.uniroma2.smoothmigration.bolt;

import java.util.Collection;
import java.util.List;

import backtype.storm.tuple.Tuple;

public class Command {
    
    public static final String STREAMID_ANCHORS_TUPLE="streamid_anchors_tuple";
    public static final String STREAMID_ANCHOR_TUPLE = "streamid_anchor_tuple";
    public static final String STREAMID_TUPLE = "streamId_tuple";
    public static final String ANCHORS_TUPLE = "anchors_tuple";
    public static final String ANCHOR_TUPLE = "anchor_tuple";
    public static final String TUPLE = "tuple";
    public static final String TASKID_STREAMID_ANCHORS_TUPLE = "taskId_streamid_anchors_tuple";
    public static final String TASKID_STREAMID_ANCHOR_TUPLE = "taskId_streamid_anchor_tuple";
    public static final String TASKID_STREAMID_TUPLE = "taskid_streamid_tuple";
    public static final String TASKID_ANCHORS_TUPLE = "taskid_anchors_tuple";
    public static final String TASKID_ANCHOR_TUPLE = "taskid_anchor_tuple";
    public static final String TASKID_TUPLE = "taskid_tuple";
    

    private String command_type;
    private String streamId;
    private Collection<Tuple> anchors;
    private int taskId;
    private List<Object> tuple;

    public Command(String command_type, String streamId, Collection<Tuple> anchors, int taskId, List<Object> tuple) {
	super();
	this.command_type = command_type;
	this.streamId = streamId;
	this.anchors = anchors;
	this.taskId = taskId;
	this.tuple = tuple;
    }

    public String toString()
    {
	return command_type+" "+streamId+" "+taskId+" "+tuple.size()+" "+anchors.size();
    }
    public String getCommand_type() {
	return command_type;
    }

    public void setCommand_tipe(String command_type) {
	this.command_type = command_type;
    }

    public String getStreamId() {
	return streamId;
    }

    public void setStreamId(String streamId) {
	this.streamId = streamId;
    }

    public Collection<Tuple> getAnchors() {
	return anchors;
    }

    public void setAnchors(Collection<Tuple> anchors) {
	this.anchors = anchors;
    }

    public int getTaskId() {
	return taskId;
    }

    public void setTaskId(int taskId) {
	this.taskId = taskId;
    }

    public List<Object> getTuple() {
	return tuple;
    }

    public void setTuple(List<Object> tuple) {
	this.tuple = tuple;
    }

}
