package it.uniroma2.adaptivescheduler.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;


public class StubNimbus implements INimbus{

	public StubNimbus() {
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, String schedulerLocalDir) { }

	@Override
	public Collection<WorkerSlot> allSlotsAvailableForScheduling(
			Collection<SupervisorDetails> existingSupervisors,
			Topologies topologies, Set<String> topologiesMissingAssignments) {

		/* Porting del codice in standalone-nimbus (nimbus.clj) */
		Collection<WorkerSlot> slots = new ArrayList<WorkerSlot>();
		
		for(SupervisorDetails s : existingSupervisors){
			Number port = (Number) s.getMeta();
			String nodeId = s.getId(); 
			WorkerSlot ws = new WorkerSlot(nodeId, port);
			slots.add(ws);
		}
		
		return slots;
	}

	@Override
	public void assignSlots(Topologies topologies,
			Map<String, Collection<WorkerSlot>> newSlotsByTopologyId) {
		
	}

	@Override
	public String getHostName(
			Map<String, SupervisorDetails> existingSupervisors, String nodeId) {

		SupervisorDetails supervisor = existingSupervisors.get(nodeId);
		if (supervisor != null)
			return supervisor.getHost();
		
		return null;
	}

	@Override
	public IScheduler getForcedScheduler() {
		return null;
	}
}
