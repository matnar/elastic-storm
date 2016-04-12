package it.uniroma2.taos.commons;
import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Logging {
	static final Logger logger = Logger.getLogger("");
	static {
		FileHandler fh;
		try {
			// This block configure the logger with handler and formatter.
			new File("/tmp/storm/log").mkdirs();
			fh = new FileHandler("/tmp/storm/log/monitor.log");		
			for (Handler h : logger.getHandlers()) {
			    logger.removeHandler(h);
			}
			logger.addHandler(fh);
			fh.setFormatter(new SingleLineFormatter());

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void append(String msg,Level level,Class c)
	{
		logger.log(level,c.getSimpleName()+" - "+ msg);
	}
}
