package it.uniroma2.adaptivescheduler.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**

 */
public class CPUMonitor{
	
    private static double lastCpuUsage = 0;
    private static Lock cpuLock = new ReentrantLock();
    private static Thread cpuMonitor = null;
    private static boolean started = false;
    
    private static int MEASUREMENT_LENGTH_SECS = 60;
    private static int INTERVAL_BETWEEN_MEASUREMENTS_SECS = 120;
    
    
    private static void startMonitor(){

    	if (!started){
        	/* Start Updater thread */
        	CPUMonitor ssr = new CPUMonitor();
        	cpuMonitor = new Thread(ssr.new CpuUsageUpdater(MEASUREMENT_LENGTH_SECS, INTERVAL_BETWEEN_MEASUREMENTS_SECS));
        	cpuMonitor.start();
        	
        	started = true;
    	}
    	
    }
    
    public static Double cpuUsage () {
    
    	startMonitor();
    	
    	cpuLock.lock();
    	try{
        	return lastCpuUsage;
    	}finally{
    		cpuLock.unlock();
    	}

    }
    
    
    private static void updateCpuUsage(double cpuUsage){
    	
    	cpuLock.lock();
    	try{
    		lastCpuUsage = cpuUsage;
    	}finally{
    		cpuLock.unlock();
    	}
    	
    }
    
    
    
    
    /**
     * SystemStatusReader is a collection of methods to read system status (cpu and memory)
     * 
     * @author Andreu Correa Casablanca
     * 
     */
    private class CpuUsageUpdater implements Runnable{
    	
        public static final int CONSERVATIVE    = 0;
        public static final int AVERAGE     = 1;
        public static final int OPTIMISTIC  = 2;
        
        private boolean useExplicitInterval = true;
        private int measurementLengthSec = 10;
        private int intervalBetweenMeasurementsSec = 30;
        
        public CpuUsageUpdater(int measurementLengthSec, int intervalBetweenMeasurementsSec) {
        	
        	this.measurementLengthSec = measurementLengthSec;
            this.intervalBetweenMeasurementsSec = intervalBetweenMeasurementsSec;
        	this.useExplicitInterval = true;
        	
        }

//        public CpuUsageUpdater(int intervalBetweenMeasurementsSec) {        	
//            this.intervalBetweenMeasurementsSec = intervalBetweenMeasurementsSec;
//        	this.useExplicitInterval = false;
//        }
       

        /**
         * cpuUsage gives us the percentage of cpu usage
         * 
         * mpstat -P ALL out stream example:
         *
         *  Linux 3.2.0-30-generic (castarco-laptop)    10/09/12    _x86_64_    (2 CPU)                 - To discard
         *                                                                                              - To discard
         *  00:16:30     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest   %idle    - To discard
         *  00:16:30     all   17,62    0,03    3,55    0,84    0,00    0,03    0,00    0,00   77,93
         *  00:16:30       0   17,36    0,05    3,61    0,83    0,00    0,05    0,00    0,00   78,12
         *  00:16:30       1   17,88    0,02    3,49    0,86    0,00    0,01    0,00    0,00   77,74
         * 
         * @param measureMode Indicates if we want optimistic, convervative or average measurements.
         */
        public Double cpuUsage (String cmd, int measureMode) throws Exception {

            BufferedReader mpstatReader = null;

            String      mpstatLine;
            String[]    mpstatChunkedLine;

            Double      selected_idle;

            try {
                Runtime runtime = Runtime.getRuntime();
                Process mpstatProcess = runtime.exec(cmd);

                mpstatReader = new BufferedReader(new InputStreamReader(mpstatProcess.getInputStream()));

                // We discard the three first lines
                mpstatReader.readLine();
                mpstatReader.readLine();

                mpstatLine = mpstatReader.readLine();
                if (mpstatLine == null) {
                    throw new Exception("mpstat didn't work well");
                }
                
                mpstatChunkedLine = mpstatLine.replaceAll(",", ".").split("\\s+");
                int index = 11;
                for(int i = 0; i < mpstatChunkedLine.length; i++){
                	if (mpstatChunkedLine[i].toLowerCase().contains("idle")){
                		index = i;
                		break;
                	}
                }
                
                mpstatLine = mpstatReader.readLine();
                if (measureMode == CpuUsageUpdater.AVERAGE) {
                    mpstatChunkedLine = mpstatLine.replaceAll(",", ".").split("\\s+");
                    selected_idle = Double.parseDouble(mpstatChunkedLine[index]);
                } else {
                    selected_idle   = (measureMode == CpuUsageUpdater.CONSERVATIVE)?200.:0.;
                    Double candidate_idle;

                    int i = 0;
                    while((mpstatLine = mpstatReader.readLine()) != null) {
                        mpstatChunkedLine = mpstatLine.replaceAll(",", ".").split("\\s+");
                        candidate_idle = Double.parseDouble(mpstatChunkedLine[index]);

                        if (measureMode == CpuUsageUpdater.CONSERVATIVE) {
                            selected_idle = (selected_idle < candidate_idle)?selected_idle:candidate_idle;
                        } else if (measureMode == CpuUsageUpdater.OPTIMISTIC) {
                            selected_idle = (selected_idle > candidate_idle)?selected_idle:candidate_idle;
                        }
                        ++i;
                    }
                    if (i == 0) {
                        throw new Exception("mpstat didn't work well");
                    }
                }
            } catch (Exception e) {
                throw e; // It's not desirable to handle the exception here
            } finally {
                if (mpstatReader != null) try {
                    mpstatReader.close();
                } catch (IOException e) {
                    // Do nothing
                }
            }

            return  100-selected_idle;
        }
        
        

        private String getCommand(){
        	if (useExplicitInterval){
        		return "mpstat " + this.measurementLengthSec + " 1";
        	} else {
            	return "mpstat -P ALL";
        	}
        }
        
        
        
    	@Override
    	public void run() {
    		
    		System.out.println("CpuUsageUpdater started...");
    		while(true){

    			try {
    				String cmd = getCommand();
    				Double utilization = this.cpuUsage(cmd, CpuUsageUpdater.AVERAGE);
    				CPUMonitor.updateCpuUsage(utilization.doubleValue());
    			} catch (Exception e) {	}
    			
    			try {
    				Thread.sleep(intervalBetweenMeasurementsSec * 1000);
    			} catch (InterruptedException e1) { }

    		}
    		
    	}
    }
}