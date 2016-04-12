package it.uniroma2.smoothmigration.spout;

import java.util.List;

public class SpoutCommand {

    public static final String STREAMID_TUPLE_MESSAGEID = "streamid_tuple_messageid";
    public static final String TUPLE_MESSAGEID = "tuple_messageid";
    public static final String TUPLE = "tuple";
    public static final String STREAMID_TUPLE = "streamid_tuple";
    public static final String TASKID_STREAMID_TUPLE_MESSAGEID = "taskid_streamid_tuple__messageid";
    public static final String TASKID_TUPLE_MESSAGEID = "taskid_tuple_messageid";
    public static final String TASKID_STREAMID_TUPLE = "taskid_string_streamid_tuple";
    public static final String TASKID_TUPLE = "taskid_tuple";

    private String command_type;
    private String streamId;
    private Object messageId;
    private int taskId;
    private List<Object> tuple;

    public SpoutCommand(String command_type, String streamId, Object messageId, int taskId, List<Object> tuple) {
	super();
	this.command_type = command_type;
	this.streamId = streamId;
	this.messageId = messageId;
	this.taskId = taskId;
	this.tuple = tuple;
    }

    public String toString() {
	return command_type + " " + streamId + " " + taskId + " " + tuple.size() + " " + messageId.toString();
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

    public Object getMessageId() {
	return messageId;
    }

    public void setMessageId(Object messageId) {
	this.messageId = messageId;
    }
}
