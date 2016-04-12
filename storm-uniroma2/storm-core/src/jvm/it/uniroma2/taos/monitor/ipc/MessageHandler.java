package it.uniroma2.taos.monitor.ipc;

import it.uniroma2.taos.commons.Constants;
import it.uniroma2.taos.commons.Logging;
import it.uniroma2.taos.commons.Utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;

import backtype.storm.Config;

public abstract class MessageHandler implements Runnable {
    private long lastUpdate = System.currentTimeMillis();
    final String basePath = System.getProperty("java.io.tmpdir") + File.separator + "storm" + File.separator + "Chronicle";
    protected ArrayList<String> queueNames = new ArrayList<String>();
    protected ArrayList<TimedReadIndexedChronicle> queues = new ArrayList<TimedReadIndexedChronicle>();
    private static Long timeout = 0l;
    private static Long check = 0l;

    public MessageHandler() {
	if (timeout == 0) {
	    Map confs = backtype.storm.utils.Utils.readStormConfig();
	    timeout = ((Integer) confs.get(Config.TAOS_QUEUE_DELETE_TIMEOUT)).longValue();
	    check = ((Integer) confs.get(Config.TAOS_QUEUE_DIRECTORY_CHECK_PERIOD)).longValue();
	}
    }

    private void updateQueues() {

	if (System.currentTimeMillis() - lastUpdate < check)
	    return;
	lastUpdate = System.currentTimeMillis();
	File folder = new File(basePath);
	File[] listOfFiles = folder.listFiles();
	if (listOfFiles == null)
	    return;
	ArrayList<String> updatedQueues = new ArrayList<String>();
	for (File file : listOfFiles) {
	    String fileName = file.getName();
	    if (file.isFile() && fileName.contains("index")) {
		fileName = fileName.replace(".index", "");
		updatedQueues.add(fileName);
	    }
	}

	for (String name : updatedQueues) {
	    if (!queueNames.contains(name)) {
		Logging.append("found new queue " + name, Level.INFO, MessageHandler.class);
		File f = new File(basePath + File.separator + name + ".lock");
		if (f.exists())
		    f.delete();
		TimedReadIndexedChronicle c = null;
		try {
		    c = new TimedReadIndexedChronicle(basePath + File.separator + name);

		} catch (IOException e) {
		    // TODO Auto-generated catch block
		    Logging.append(Utils.stringStackTrace(e), Level.SEVERE, this.getClass());
		}
		queueNames.add(name);
		c.useUnsafe();
		queues.add(c);
	    }
	}
    }

    public void remove(TimedReadIndexedChronicle c) {

	queues.remove(c);
	queueNames.remove(c.name());
	c.closeDelete();
    }

    public void run() {

	while (true) {
	    updateQueues();
	    ArrayList<TimedReadIndexedChronicle> keep = new ArrayList<TimedReadIndexedChronicle>();
	    for (TimedReadIndexedChronicle c : queues) {
		File f = new File(basePath + File.separator + c.name() + ".lock");
		if (f.exists()) {
		    c.close();
		    c.getExcerpt().close();
		    onTimeOut(c);
		    queueNames.remove(c.name());
		    Logging.append("Stale queue detected, removing old queue " + c.name(), Level.INFO, MessageHandler.class);
		} else if (System.currentTimeMillis() - c.getLastMessage() < timeout) {
		    while (c.getExcerpt().nextIndex()) {
			handleMessage(c.getExcerpt().readByteString(), c);
			c.setLastMessage(System.currentTimeMillis());
		    }
		    keep.add(c);
		} else {

		    c.closeDelete();
		    onTimeOut(c);
		    queueNames.remove(c.name());
		    Logging.append("Removing for timeout " + c.name(), Level.INFO, MessageHandler.class);
		}
		queues = keep;
	    }
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }

	}

    }

    public abstract void onTimeOut(TimedReadIndexedChronicle c);

    public abstract void handleMessage(String message, TimedReadIndexedChronicle c);
}
