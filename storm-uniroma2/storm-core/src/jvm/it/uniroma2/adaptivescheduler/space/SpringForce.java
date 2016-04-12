package it.uniroma2.adaptivescheduler.space;

import java.util.Random;

public class SpringForce {

	private double[] versor;
	private double magnitude;
	private Space space;

	public SpringForce() {
		space = SpaceFactory.createSpace();
		
		int dim = space.getTotalDimensions();
		versor = new double[dim];
		
		for (int i = 0; i < dim; i++){
			versor[i] = 0;
		}

		magnitude = 0;
	}
	
	public SpringForce(Point a, Point b, double datarate) {

		space = SpaceFactory.createSpace();

		versor = this.computeVersor(a, b);
		magnitude = this.computeMagnitude(a, b, datarate);
		
	}

	public SpringForce(Point a, Point b) {

		space = SpaceFactory.createSpace();

		versor = this.computeVersor(a, b);
		magnitude = this.computeMagnitude(a, b, 1.0);
		
	}
	
	private double[] computeVersor(Point a, Point b) {

		double[] versor = new double[space.getTotalDimensions()];

		double sum = 0;
		for (int i = 0; i < space.getLatencyDimensions(); i++) {
			versor[i] = a.get(i) - b.get(i);
			sum += Math.abs(versor[i]);
		}

		if (sum == 0) {
			/* two points are in the same location */
			Random rnd = new Random();

			for (int i = 0; i < space.getLatencyDimensions(); i++) {
				versor[i] = rnd.nextDouble();
				sum += Math.abs(versor[i]);
			}
		}

		/* Set value only for latency dimensions */
		for (int i = 0; i < space.getLatencyDimensions(); i++) {
			versor[i] = versor[i] / sum;
		}
		for (int i = space.getLatencyDimensions(); i < space.getTotalDimensions(); i++) {
			versor[i] = 0;
		}

		return versor;

	}

	private double computeMagnitude(Point a, Point b, double datarate) {
		return (space.distance(a, b) * datarate);
	}

	public void multiply(double factor) {
		magnitude = magnitude * factor;
	}
	
	public void add(SpringForce otherForce){

		if (otherForce == null)
			return;
		
		/* Sum vectors */
		double sum = 0;
		double squareSum = 0;
		for (int i = 0; i < versor.length; i++) {
			versor[i] = (versor[i] * magnitude) + (otherForce.versor[i] * otherForce.magnitude);
			sum += Math.abs(versor[i]);
			squareSum += Math.pow(versor[i], 2.0);
		}

		/* Compute magnitude */
		if (squareSum == 0.0)
			magnitude = 0.0;
		else
			magnitude = Math.sqrt(squareSum);

		/* Compute versor */
		if (sum > 0){
			for (int i = 0; i < versor.length; i++) {
				versor[i] = versor[i] / sum;
			}
		}
		
	}
	
	public boolean lessThan(double threshold){
		return (magnitude < threshold);
	}
	public boolean greaterThan(double threshold){
		return (magnitude > threshold);
	}
	public boolean isNull(){
		return (magnitude == 0);
	}

	public Point movePoint(Point p, double time) {

		if (p == null || p.getDimensionality() != versor.length)
			return null;

		Point newPosition = new Point(p.getDimensionality());
		for (int i = 0; i < p.getDimensionality(); i++) {
			newPosition.set(i, p.get(i) + time * magnitude * versor[i]);
		}

		return newPosition;

	}

	@Override
	public String toString() {
		String str = "|" + magnitude + "|*u(";
		for (int i = 0; i < versor.length; i++) {
			str += versor[i] + ", ";
		}
		str += ")";
		return str;
	}

	
}
