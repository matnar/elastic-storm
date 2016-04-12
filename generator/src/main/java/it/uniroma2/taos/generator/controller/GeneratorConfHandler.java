package it.uniroma2.taos.generator.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class GeneratorConfHandler {

    /*
     * Port of Mongo DB for metrics retrieval
     */
    private static final String MONGO_PORT_PROPNAME = "taos.mongo_db_port";

    /*
     * IP of Mongo DB for metrics retrieval
     */
    private static final String MONGO_IP_PROPNAME = "taos.mongo_db_ip";

    /*
     * Node consolidation factor, with N nodes, a maximum of gamma*(TotalExecutorNumber/N) executors are allowed on each node
     */
    private static final String GAMMA_PROPNAME = "taos.gamma";
    /*
     * Convenience value, placement where the performance improvement is under this threshold are marked to be ignored.
     */ 
    private static final String CONVENIENCE_FACTOR_PROPNAME = "taos.convenience_factor";
    /*
     * Parameter for exponential smoothing on convenience value
     */
    private static final String CONVENIENCE_SMOOTHING_PROPNAME = "taos.convenience_smoothing";
    /*
     * Tollerance for a node saturation, when detecting node saturation SATURATION_TOLLERANCE+NODE_TARGET_UTILIZATION is 
     * used as maximum node  utilization.
     */
    private static final String SATURATION_TOLLERANCE_PROPNAME = "taos.saturation_tollerance";
    /*
     * Whene cluster utilization, normalized against SATURATION_TOLLERANCE+NODE_TARGET_UTILIZATION is
     * above this threshold. All placement are  marked as non convenient even if some node is saturated.
     * This ensures stability
     */
    
    private static final String SATURATION_THRESHOLD_PROPNAME = "taos.saturation_threshold";
    
    public static int MONGO_PORT = 27017;
    public static String MONGO_IP = "localhost";
    public static double GAMMA = 1.0;
    public static double CONVENIENCE_FACTOR = 0.25;
    public static double DAMPING_FACTOR = 0.5;
    public static double SATURATION_TOLLERANCE = 0.1;
    public static double SATURATION_THRESHOLD = 0.8;
    private static final Logger logger = LogManager.getLogger(GeneratorConfHandler.class.getName());

    static {
	try {
	    File f = new File("config.properties");
	    if (!f.exists()) {

		f.createNewFile();

	    }
	    Properties prop = new Properties();
	    InputStream input = null;
	    input = new FileInputStream("config.properties");
	    prop.load(input);
	    MONGO_PORT = Integer.parseInt(prop.getProperty(MONGO_PORT_PROPNAME, "27017"));
	    MONGO_IP = prop.getProperty(MONGO_IP_PROPNAME, "localhost");
	    GAMMA = Double.parseDouble(prop.getProperty(GAMMA_PROPNAME, "1.0"));
	    CONVENIENCE_FACTOR = Double.parseDouble(prop.getProperty(CONVENIENCE_FACTOR_PROPNAME, "0.25"));
	    DAMPING_FACTOR = Double.parseDouble(prop.getProperty(CONVENIENCE_SMOOTHING_PROPNAME, "0.5"));
	    SATURATION_TOLLERANCE = Double.parseDouble(prop.getProperty(SATURATION_TOLLERANCE_PROPNAME, "0.1"));
	    SATURATION_THRESHOLD = Double.parseDouble(prop.getProperty(SATURATION_THRESHOLD_PROPNAME, "0.8"));
	    input.close();
	    f.delete();
	    f.createNewFile();
	    prop = new Properties();
	    OutputStream output = null;
	    output = new FileOutputStream(f);
	    prop.setProperty(MONGO_PORT_PROPNAME, MONGO_PORT + "");
	    prop.setProperty(MONGO_IP_PROPNAME, MONGO_IP);
	    prop.setProperty(GAMMA_PROPNAME, GAMMA + "");
	    prop.setProperty(CONVENIENCE_FACTOR_PROPNAME, CONVENIENCE_FACTOR + "");
	    prop.setProperty(CONVENIENCE_SMOOTHING_PROPNAME, DAMPING_FACTOR + "");
	    prop.setProperty(SATURATION_TOLLERANCE_PROPNAME, SATURATION_TOLLERANCE + "");
	    prop.setProperty(SATURATION_THRESHOLD_PROPNAME, SATURATION_THRESHOLD + "");
	    prop.store(output, "Configuration file for generator service");
	    output.close();

	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	// load a properties file
	catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

    }

}
