package it.uniroma2.adaptivescheduler.space;

import java.security.InvalidParameterException;

/**
 * Point of the network space
 * 
 * This class represents a point inside the network space, it stores its coordinates
 * and exposes simple functions to deal with them . 
 * 
 * @author Matteo Nardelli
 *
 */
public class Point {

	private double[] coordinates;

	/**
	 * Create a new point. 
	 * 
	 * @param dimensionality
	 */
	public Point(int dimensionality) {
		coordinates = new double[dimensionality];
		
		for (int i = 0; i < coordinates.length; i++) {
			coordinates[i] = 0.0;
		}
	
	}

	/**
	 * Create a new point. 
	 * 
	 * @param initialCoordinates
	 */
	public Point(double[] initialCoordinates) {

		if (initialCoordinates == null) {
			throw new InvalidParameterException("Invalid initialCoordinates parameter");
		} else {
			coordinates = new double[initialCoordinates.length];

			for (int i = 0; i < coordinates.length; i++) {
				coordinates[i] += initialCoordinates[i];
			}
		}

	}

	/**
	 * Create a new point. 
	 * 
	 * @param otherPoint
	 */
	public Point(Point otherPoint) {

		if (otherPoint == null) {
			throw new InvalidParameterException("Other Point is null");
		} else {
			coordinates = new double[otherPoint.getDimensionality()];

			for (int i = 0; i < coordinates.length; i++) {
				coordinates[i] += otherPoint.get(i);
			}
		}

	}

	/**
	 * Set the value associated to a dimension 
	 * 
	 * @param dimension
	 * @param value
	 */
	public void set(int dimension, double value) {
		if (dimension < coordinates.length) {
			coordinates[dimension] = value;
		}
	}

	
	/**
	 * Get the value associated to a dimension 
	 * 
	 * @param dimension
	 * @return
	 */
	public double get(int dimension) {
		if (dimension < coordinates.length) {
			return coordinates[dimension];
		}
		return 0;
	}

	/**
	 * Get the dimensionality of the Point. 
	 * 
	 * It should be equal to the dimensionality of the space. 
	 * @return
	 */
	public int getDimensionality() {
		return coordinates.length;
	}

//	public Point getInverse() {
//
//		Point inverse = new Point(coordinates.length);
//
//		for (int i = 0; i < coordinates.length; i++)
//			inverse.set(i, -coordinates[i]);
//
//		return inverse;
//	}

	@Override
	public String toString() {
		String output = "(";
		for (int i = 0; i < coordinates.length; i++) {
			output += coordinates[i] + (i == coordinates.length - 1 ? "" : ",");
		}
		output += ")";
		return output;
	}

	public Point clone(){
		return new Point(coordinates);
	}
}
