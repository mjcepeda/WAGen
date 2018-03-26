package edu.rit.wagen.test;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class DistributionTest {

	@Test
	public void test2() {
		int count = 5;
		int sum = 15;
		java.util.Random g = new java.util.Random();

		int vals[] = new int[count];
		sum -= count;

		for (int i = 0; i < count - 1; ++i) {
			vals[i] = g.nextInt(sum);
		}
		vals[count - 1] = sum;

		java.util.Arrays.sort(vals);
		for (int i = count - 1; i > 0; --i) {
			vals[i] -= vals[i - 1];
		}
		for (int i = 0; i < count; ++i) {
			++vals[i];
		}

		for (int i = 0; i < count; ++i) {
			System.out.printf("%4d", vals[i]);
		}
		System.out.printf("\n");

	}

//	@Test
	public void test() {
		int[] values = getRandDistArray(2, 4, 1, 2);
		Arrays.asList(values).forEach(System.out::println);
		;
	}

	private int[] getRandDistArray(int n, int m, int min, int max) {
		int randArray[] = new int[n];
		int sum = 0;

		// Generate n random numbers
		for (int i = 0; i < randArray.length; i++) {
			randArray[i] = getRandomNumberInRange(min, max);
			sum += randArray[i];
		}

		// Normalize sum to m
		for (int i = 0; i < randArray.length; i++) {
			randArray[i] /= sum;
			randArray[i] *= m;
		}
		return randArray;
	}

	private int getRandomNumberInRange(int min, int max) {

		if (min >= max) {
			throw new IllegalArgumentException("max must be greater than min");
		}

		Random r = new Random();
		return r.nextInt((max - min) + 1) + min;
	}
}
