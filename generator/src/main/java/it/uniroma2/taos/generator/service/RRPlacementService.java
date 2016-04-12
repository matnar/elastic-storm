package it.uniroma2.taos.generator.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import it.uniroma2.taos.commons.network.ExecutorTopo;
import it.uniroma2.taos.commons.network.NodeSlots;
import it.uniroma2.taos.commons.network.Assignment;
import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;

@Component
public class RRPlacementService implements PlacementService {

	static final Logger logger = LogManager.getLogger(RRPlacementService.class.getName());

	@Override
	public PlacementResponse getPlacement(PlacementRequest pr) {

		ArrayList<ExecutorTopo> elist = pr.getExecutors();
		Iterator<NodeSlots> roundRobinIterator = pr.getAvailableNodes().iterator();
		PlacementResponse response = new PlacementResponse();
		response.setIgnoreIfDeployed(true);
		for (ExecutorTopo executor : elist) {
			HashMap<String, Integer> slots = response.countUsedSlots(executor.getTopoId());
			NodeSlots node=null;
			boolean found = false;
			boolean rst = false;
			while (!found) {
				if (!roundRobinIterator.hasNext()) {
					if (rst == true)
						break;
					roundRobinIterator = pr.getAvailableNodes().iterator();
					rst = true;
					continue;
				}
				node = roundRobinIterator.next();
				if (slots.containsKey(node.getNodeId()) && node.getAvailableSlots() - slots.get(node.getNodeId()) <= 0)
					continue;
				found = true;
			}
			if (rst == true && found == false) {
				logger.info("No feasible deployment found, returning empty response");
				return new PlacementResponse();
			}
			Assignment exists=response.getPlacementByNodeAndTopo(node.getNodeId(),executor.getTopoId());
			if(exists==null)
			{
				ArrayList<String> list=new ArrayList<String>();
				list.add(executor.getExeId());
				Assignment place=new Assignment(node.getNodeId(), executor.getTopoId(), list);
				response.getAssignment().add(place);
			}
			else
			{
				exists.getExecutorsIds().add(executor.getExeId());
			}

		}

		logger.info("Response is: "+ response.toString());
		return response;
	}

}
