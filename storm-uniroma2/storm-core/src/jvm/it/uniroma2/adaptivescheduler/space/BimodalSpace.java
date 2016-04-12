package it.uniroma2.adaptivescheduler.space;

public interface BimodalSpace extends Space {

	public double distance(Point a, Point b, boolean useOtherDimensions);

}
