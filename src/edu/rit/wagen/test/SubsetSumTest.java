package edu.rit.wagen.test;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import edu.rit.wagen.utils.SubsetSum;

public class SubsetSumTest {

	@Test
	public void test() {
		System.out.println("Test 1");
		List<Long> listKs = Arrays.asList(1L,2L,5L,6L,13L,27L,44L,47L,48L);
		SubsetSum.appOverweightSubsetSum(listKs, 30, 0.1).forEach(System.out::println);
	}

	@Test
	public void test2() {
		System.out.println("Test 2");
		List<Long> listKs = Arrays.asList(5L,9L,3L,1L);
		SubsetSum.appOverweightSubsetSum(listKs, 7, 0.1).forEach(System.out::println);
	}

	
	@Test
	public void test3() {
		System.out.println("Test 3");
		List<Long> listKs = Arrays.asList(5L,9L,2L,2L);
		SubsetSum.appOverweightSubsetSum(listKs, 4, 0.1).forEach(System.out::println);
	}

}
