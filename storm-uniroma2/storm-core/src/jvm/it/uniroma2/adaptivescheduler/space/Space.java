package it.uniroma2.adaptivescheduler.space;


/**
 * Space of cost used by the Vivaldi Algorithm. 
 * 
 * 
 * @author Matteo Nardelli
 */
public interface Space {
	
	public int getLatencyDimensions();
	public int getTotalDimensions();
	
	/**
	 * Difference between two points 
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public Point difference(Point a, Point b);
	
	/**
	 * Return the <b>norm</b> of a vector (the point)
	 * 
	 * @param a
	 * @return
	 */
	public double norm(Point a);
	
	
	/**
	 * Calculate the distance between two points 
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	public double distance(Point a, Point b);
	
	/**
	 * Multiply the value associated to the dimensions for the 
	 * point by the constant <em>a</em>.
	 * @param a
	 * @param p
	 * @return
	 */
	public Point multiply(double a, Point p);

}
