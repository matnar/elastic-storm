package it.uniroma2.adaptivescheduler.vivaldi;

import it.uniroma2.adaptivescheduler.space.Point;
import it.uniroma2.adaptivescheduler.space.Space;
import it.uniroma2.adaptivescheduler.space.SpaceFactory;

import java.util.Random;

public class VivaldiForce {
	private double[] versor;
	private double magnitude;
	private Space space;
	
	public VivaldiForce(double latency, Point a, Point b) {

		space = SpaceFactory.createSpace();

		versor = this.computeVersor(a, b);
		magnitude = this.computeMagnitude(latency, a, b);
		
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

	private double computeMagnitude(double latency, Point a, Point b) {
		return (latency - space.distance(a, b));
	}

	public void multiply(double factor) {
		magnitude = magnitude * factor;
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