package it.uniroma2.adaptivescheduler.space;

public class LatencyPlusOneSpace implements BimodalSpace{

	private static int latencyDimensions = 2;
	private static int dimensions = 3;
	
	private static double[] weights = {1,1,1};
	

	public LatencyPlusOneSpace() {
	}

	public static double[] getWeights() {
		return weights;
	}
	public static void setWeights(double[] weights) {
		LatencyPlusOneSpace.weights = weights;
	}

	@Override
	public Point difference(Point a, Point b) {
		
		if(a == null || b == null)
			return null;
		
		int dim = Math.min(latencyDimensions, a.getDimensionality());
		Point newPoint = new Point(dimensions);
		
		/* Latency components */
		for (int i = 0; i < dim; i++){
			newPoint.set(i, (a.get(i) - b.get(i)));
		}

		/* Utilization components */
		if (a.getDimensionality() > latencyDimensions){
			for(int i = dim; i < dimensions; i++){
				newPoint.set(i, 0);
			}
		}

		return newPoint;
	}

	@Override
	public double norm(Point a) {
		
		if (a == null)
			return Double.NaN;
		
		double squareSum = 0;

		/* Compute norm using just latency dimensions */
		int dim = Math.min(a.getDimensionality(), latencyDimensions);
		for (int i = 0; i < dim; i++)
			squareSum += Math.pow(a.get(i), 2.0);
		
		squareSum = Math.sqrt(squareSum);
				
		return squareSum;
	}

	@Override
	public Point multiply(double a, Point p) {
		
		if(p == null)
			return null;
		
		int dim = Math.min(p.getDimensionality(), latencyDimensions);
		Point newPoint = new Point(dimensions);
		
		for (int i = 0; i < dim; i++){
			newPoint.set(i, (a * p.get(i)));
		}

		/* Utilization components */
		for(int i = dim; i < p.getDimensionality(); i++){
			newPoint.set(i, p.get(i));
		}
		for(int i = p.getDimensionality(); i < dimensions; i++){
			newPoint.set(i, 0);
		}

		return newPoint;
		
	}

	@Override
	public double distance(Point a, Point b) {

		if (a == null || b == null)
			return Double.NaN;
		
		double squareSum = 0;
		
		int dim = Math.min(a.getDimensionality(), latencyDimensions);
		
		for (int i = 0; i < dim; i++)
			squareSum += Math.pow((a.get(i) - b.get(i)), 2.0);
		
		squareSum = Math.sqrt(squareSum);
				
		return squareSum;

	}

	public double distance(Point a, Point b, boolean useUtilization) {
		
		if (useUtilization == false)
			return distance(a, b);

		if (a == null || b == null)
			return Double.NaN;
		
		double squareSum = 0;
		
		int dim = Math.min(a.getDimensionality(), dimensions);
		
		for (int i = 0; i < dim; i++)
			squareSum += Math.pow(weights[i] * (a.get(i) - b.get(i)), 2.0);
		
		squareSum = Math.sqrt(squareSum);
				
		return squareSum;

	}

	
	@Override
	public int getLatencyDimensions() {
		return latencyDimensions;
	}
	
	@Override
	public int getTotalDimensions() {
		return dimensions;
	}

	
}
