package it.uniroma2.taos.commons.network;

import it.uniroma2.taos.commons.dao.TrafficTuple;
import it.uniroma2.taos.commons.persistence.MongoManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class PlacementResponse {
    private ArrayList<Assignment> assignment = new ArrayList<Assignment>();
    private boolean ignoreIfDeployed = false;

    // (topoId---->(componentId--->(StreamId---->[tasks])))
    private HashMap<String, HashMap<String, HashMap<String, ArrayList<Integer>>>> streams = new HashMap<String, HashMap<String, HashMap<String, ArrayList<Integer>>>>();
    private HashMap<String, List<Integer>> componentToTask = new HashMap<String, List<Integer>>();
    private HashMap<String, String> HostToNodeId = new HashMap<String, String>();



    public PlacementResponse() {
	super();
    }

    public ArrayList<String> getTopos() {
	ArrayList<String> toReturn = new ArrayList<String>();
	for (Assignment assign : assignment) {
	    if (!toReturn.contains(assign.getTopoId())) {
		toReturn.add(assign.getTopoId());
	    }
	}
	return toReturn;
    }

    public Assignment getAssignmentByPortAndNode(Integer port, String nodeId) {
	for (Assignment placement : assignment) {
	    if (placement.getPort() == port && placement.getNodeId().equals(nodeId)) {
		return placement;
	    }
	}
	return null;

    }

    public Assignment getPlacementByNodeAndTopo(String nodeId, String topoId) {
	for (Assignment placement : assignment) {
	    if (placement.getNodeId().equals(nodeId) && placement.getTopoId().equals(topoId))
		return placement;
	}
	return null;
    }

    public void RemoveByTopology(String topoid) {
	ArrayList<Assignment> keep = new ArrayList<Assignment>();
	for (Assignment placement : assignment) {
	    if (placement.getTopoId().equals(topoid))
		continue;
	    keep.add(placement);
	}
	assignment = keep;
    }

    public ArrayList<Assignment> getByTopology(String topoid) {
	ArrayList<Assignment> toReturn = new ArrayList<Assignment>();
	for (Assignment placement : assignment) {
	    if (topoid.equals(placement.getTopoId()))
		toReturn.add(placement);
	}
	return toReturn;
    }

    public ArrayList<ExecutorTopo> getExecutorsOnNode(String nodeId) {
	ArrayList<ExecutorTopo> toReturn = new ArrayList<ExecutorTopo>();
	for (Assignment pl : assignment) {
	    if (pl.getNodeId().equals(nodeId)) {
		for (String executor : pl.getExecutorsIds()) {
		    toReturn.add(new ExecutorTopo(pl.getTopoId(), executor));
		}
	    }
	}
	return toReturn;
    }

    public ArrayList<String> getPlacedNodes() {
	ArrayList<String> toreturn = new ArrayList<String>();
	for (Assignment placement : assignment) {
	    if (!toreturn.contains(placement.getNodeId()))
		toreturn.add(placement.getNodeId());
	}
	return toreturn;
    }

    public Assignment getPlacementByPlacement(Assignment p) {
	int index = assignment.indexOf(p);
	if (index == -1)
	    return null;
	else
	    return assignment.get(index);
    }

    public HashMap<String, Integer> countUsedSlots(String excludeTopoId) {
	HashMap<String, Integer> counts = new HashMap<String, Integer>();
	ArrayList<String> nodelist = new ArrayList<String>();
	for (Assignment placement : assignment) {
	    if (!nodelist.contains(placement.getNodeId()))
		nodelist.add(placement.getNodeId());
	}
	for (String node : nodelist) {
	    for (Assignment placement : assignment) {
		if (placement.getNodeId().equals(node)) {
		    if (!placement.getTopoId().equals(excludeTopoId)) {
			Integer count = counts.get(node);
			if (count != null)
			    counts.put(node, count++);
			else {
			    counts.put(node, 1);
			}
		    }
		}
	    }

	}
	return counts;

    }

    public ArrayList<Assignment> getAssignment() {
	return assignment;
    }

    public void setAssignment(ArrayList<Assignment> assignment) {
	this.assignment = assignment;
    }

    public boolean isIgnoreIfDeployed() {
	return ignoreIfDeployed;
    }

    public void setIgnoreIfDeployed(boolean ignoreIfDeployed) {
	this.ignoreIfDeployed = ignoreIfDeployed;
    }

    public String toString() {
	return "IgnoreIFdeployed: " + ignoreIfDeployed + "  " + assignment.toString();
    }

    public ExecutorTopo getExecutorByTask(int taskId, String topo) {
	for (Assignment pl : getAssignment()) {
	    if (!pl.getTopoId().equals(topo))
		continue;
	    for (String id : pl.getExecutorsIds()) {
		String nosquare = id.substring(1, id.length() - 1);
		int beginIndex = Integer.parseInt(nosquare.split(" ")[0]);
		int endIndex = Integer.parseInt(nosquare.split(" ")[1]);
		if (beginIndex <= taskId && taskId <= endIndex) {
		    return new ExecutorTopo(topo, id);
		}
	    }
	}
	return null;
    }

    public ArrayList<TaskTopo> getInputTasks(int taskId, String stormId) {
	ArrayList<TaskTopo> taskInputTasks = new ArrayList<TaskTopo>();
	ArrayList<String> InputComponents = getInputComponents(stormId, taskId);
	for (String comp : InputComponents) {
	    List<Integer> tasks = componentToTask.get(comp);
	    for (Integer task : tasks) {
		TaskTopo toadd = new TaskTopo(task, stormId);
		if (!taskInputTasks.contains(toadd))
		    taskInputTasks.add(toadd);
	    }
	}

	return taskInputTasks;
    }

    public ArrayList<String> getInputComponents(String stormId, Integer taskId) {
	ArrayList<String> toReturn = new ArrayList<String>();
	HashMap<String, HashMap<String, ArrayList<Integer>>> comps = streams.get(stormId);
	if (streams.get(stormId) == null)
	    return toReturn;
	for (String comp : comps.keySet()) {
	    HashMap<String, ArrayList<Integer>> streamTask = comps.get(comp);
	    for (ArrayList<Integer> task : streamTask.values()) {
		if (task.contains(taskId) && !toReturn.contains(comp)) {
		    toReturn.add(comp);
		    continue;
		}
	    }
	}
	return toReturn;
    }

    public ArrayList<TaskTopo> getOutputTasks(String stormId, String componentId) {
	ArrayList<TaskTopo> outputTask = new ArrayList<TaskTopo>();
	if (streams.get(stormId) == null)
	    return outputTask;
	if (streams.get(stormId).get(componentId) == null)
	    return outputTask;
	for (ArrayList<Integer> tasks : streams.get(stormId).get(componentId).values()) {
	    for (Integer task : tasks) {
		TaskTopo toadd = new TaskTopo(task, stormId);
		if (!outputTask.contains(toadd))
		    outputTask.add(toadd);
	    }
	}
	return outputTask;
    }

    public ArrayList<TaskStream> getOutputTaskStreams(String stormId, String componentId) {
	ArrayList<TaskStream> outputTask = new ArrayList<TaskStream>();
	for (String stream : streams.get(stormId).get(componentId).keySet()) {
	    ArrayList<Integer> tasks = streams.get(stormId).get(componentId).get(stream);
	    for (Integer task : tasks) {
		TaskStream toadd = new TaskStream(task, stream);
		if (!outputTask.contains(toadd))
		    outputTask.add(toadd);
	    }
	}
	return outputTask;
    }

    public ArrayList<Integer> getTasksByExecutorId(String executorId) {
	String split = (executorId.split("@"))[0];
	split = split.substring(1, split.length() - 1);
	ArrayList<Integer> toReturn = new ArrayList<Integer>();
	for (int i = Integer.parseInt(split.split(" ")[0]); i <= Integer.parseInt(split.split(" ")[1]); i++) {
	    if (!toReturn.contains(i))
		toReturn.add(i);
	}
	return toReturn;

    }

    public HashMap<String, HashMap<String, HashMap<String, ArrayList<Integer>>>> getStreams() {
	return streams;
    }

    public void setStreams(HashMap<String, HashMap<String, HashMap<String, ArrayList<Integer>>>> streams) {
	this.streams = streams;
    }

    public HashMap<String, List<Integer>> getComponentToTask() {
	return componentToTask;
    }

    public void setComponentToTask(HashMap<String, List<Integer>> componentToTask) {
	this.componentToTask = componentToTask;
    }
    
    public HashMap<String, String> getHostToNodeId() {
        return HostToNodeId;
    }

    public void setHostToNodeId(HashMap<String, String> hostToNodeId) {
        HostToNodeId = hostToNodeId;
    }

}
