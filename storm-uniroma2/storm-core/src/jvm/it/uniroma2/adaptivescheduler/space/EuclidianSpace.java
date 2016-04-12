package it.uniroma2.adaptivescheduler.space;

public class EuclidianSpace implements Space {

	private static int dimensionality = 2;
	
	public EuclidianSpace() {
	}
	
	@Override
	public Point difference(Point a, Point b) {
		
		if(a == null || b == null)
			return null;
		
		int dim = a.getDimensionality();
		Point newPoint = new Point(dim);
		
		for (int i = 0; i < dim; i++){
			newPoint.set(i, (a.get(i) - b.get(i)));
		}

		return newPoint;
	}

	@Override
	public double norm(Point a) {
		
		if (a == null)
			return Double.NaN;
		
		double squareSum = 0;
		
		for (int i = 0; i < a.getDimensionality(); i++)
			squareSum += Math.pow(a.get(i), 2.0);
		
		squareSum = Math.sqrt(squareSum);
				
		return squareSum;
	}

	@Override
	public Point multiply(double a, Point p) {
		
		if(p == null)
			return null;
		
		int dim = p.getDimensionality();
		Point newPoint = new Point(dim);
		
		for (int i = 0; i < dim; i++){
			newPoint.set(i, (a * p.get(i)));
		}

		return newPoint;
		
	}

	@Override
	public double distance(Point a, Point b) {

		if (a == null || b == null)
			return Double.NaN;
		
		double squareSum = 0;
		
		for (int i = 0; i < a.getDimensionality(); i++)
			squareSum += Math.pow((a.get(i) - b.get(i)), 2.0);
		
		squareSum = Math.sqrt(squareSum);
				
		return squareSum;

	}

	@Override
	public int getLatencyDimensions() {
		return dimensionality;
	}
	
	@Override
	public int getTotalDimensions() {
		return getLatencyDimensions();
	}

}
