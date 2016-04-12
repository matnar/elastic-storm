package it.uniroma2.adaptivescheduler.space;

public class SpaceFactory {
	
	private static boolean useLatencyUtilizationSpace = false;
	
	public static void setUseExtendedSpace(boolean value){
		useLatencyUtilizationSpace = value;
	}
		
	public static Space createSpace(){
		if (useLatencyUtilizationSpace){
			return new LatencyPlusOneSpace();
		}
		return new EuclidianSpace();
	}

}
