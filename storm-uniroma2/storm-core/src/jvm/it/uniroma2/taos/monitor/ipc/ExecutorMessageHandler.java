package it.uniroma2.taos.monitor.ipc;

import it.uniroma2.taos.monitor.stats.ExecutorHandler;
import it.uniroma2.taos.monitor.stats.ExecutorInfo;
import it.uniroma2.taos.commons.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.sql.rowset.spi.SyncFactory;

public class ExecutorMessageHandler extends MessageHandler {
	private HashMap<String, ExecutorHandler> data = new HashMap<String, ExecutorHandler>();

	public ArrayList<ExecutorInfo> retrieveDataStats() {
		ArrayList<ExecutorInfo> tmp = new ArrayList<ExecutorInfo>();
		synchronized (data) {
			for (Entry<String, ExecutorHandler> e : data.entrySet()) {
				tmp.add(e.getValue().retrieve());
			}
		}
		return tmp;
	}
	@Override
	public void handleMessage(String message, TimedReadIndexedChronicle c) {
		synchronized (data) {
			String[] messageSplits = message.split("_");
			String executorId = c.getExectorId();
			
			if (messageSplits[0].equals(Constants.TUPLE_NOTIFY)) {
				String src = messageSplits[1];
				if (data.containsKey(executorId)) {
					data.get(executorId).increaseCounter(src);
				} else {
					ExecutorHandler toPut = new ExecutorHandler(executorId);
					toPut.increaseCounter(src);
					data.put(executorId, toPut);
				}
			} else if (messageSplits[0].equals(Constants.CLOSE_MESSAGE)) {
				remove(c);
				data.remove(executorId);
			} else if (messageSplits[0].equals(Constants.ACK)) {
			} else if (messageSplits[0].equals(Constants.CPUTIME)) {
				long curCpu = Long.parseLong(messageSplits[1]);
				if (!data.containsKey(executorId)) {
					ExecutorHandler toPut = new ExecutorHandler(executorId);
					toPut.setCurCpu(curCpu);
					data.put(executorId, toPut);
				}
				data.get(executorId).setCurCpu(curCpu);
			}

		}

	}

	@Override
	public void onTimeOut(TimedReadIndexedChronicle c) {
		synchronized (data) {
			data.remove(c.getExectorId());
		}
	}

}
