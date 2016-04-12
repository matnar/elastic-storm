package it.uniroma2.taos.monitor.ipc;

import java.io.File;
import java.io.IOException;
import java.security.spec.ECField;

import com.higherfrequencytrading.chronicle.Excerpt;
import com.higherfrequencytrading.chronicle.impl.IndexedChronicle;

public class TimedReadIndexedChronicle extends IndexedChronicle {

	final Excerpt excerpt;

	private String path;
	private long LastMessage = System.currentTimeMillis();
	private String exectorId;

	public TimedReadIndexedChronicle(String basePath) throws IOException {
		super(basePath);
		this.path = basePath;
		excerpt = this.createExcerpt();
		excerpt.index(0);
		while (excerpt.hasNextIndex()) {
			excerpt.nextIndex();
		}
		exectorId = name();

	}

	public String getExectorId() {
		return exectorId;
	}

	public void setExectorId(String exectorId) {
		this.exectorId = exectorId;
	}

	public long getLastMessage() {
		return LastMessage;
	}

	public void setLastMessage(long lastMessage) {
		LastMessage = lastMessage;
	}

	public Excerpt getExcerpt() {
		return excerpt;
	}

	public void closeDelete() {
		this.close();
		excerpt.close();
		File f = new File(path + ".index");
		f.delete();
		f = new File(path + ".data");
		f.delete();

	}

}
