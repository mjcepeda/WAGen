package edu.rit.wagen.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The Class SubsetSum.
 * 
 * @author Maria Cepeda
 */
public class SubsetSum {

	/**
	 * App overweight subset sum.
	 *
	 * @param listKs the list ks
	 * @param target the target
	 * @param ratio the ratio
	 * @return the list
	 * @throws RuntimeException the runtime exception
	 */
	public static List<Long> appOverweightSubsetSum(List<Long> listKs, int target, double ratio)
			throws RuntimeException {
		List<Long> subset = new ArrayList<>();
		// sort the elements in ascending order c_i < c_i+1
		Collections.sort(listKs);
		Optional<Long> ci = listKs.stream().filter(i -> i >= target).findFirst();
		// trim the list by removing elements c_i+1,...,cm
		if (ci.isPresent()) {
			listKs = listKs.stream().filter(i -> i <= ci.get()).collect(Collectors.toList());
		}
		// set the largest possible optimal solution
		int p = 0;
		int index = -1;
		while (p < target && index < listKs.size()) {
			index++;
			p += listKs.get(index);
		}
		if (!(index < listKs.size()) && p < target) {
			throw new RuntimeException("DP - No solution existis");
		} else if (index < listKs.size() && listKs.get(index) >= target) {
			subset.add(listKs.get(index));
			return subset;
		}

		// set the quantization factor
		double d = Math.pow(ratio * 0.5, 2) * p;
		// set number of buckets
		int g = (int) ((int) Math.ceil(p / d) + Math.min(index + 1, Math.ceil(2 / ratio)));
		List<Long>[] buckets = new ArrayList[g];
		Arrays.fill(buckets, new ArrayList());

		long[] x = new long[g + 1];
		// init the array
		Arrays.fill(x, -1);
		x[0] = 0;
		final double splitValue = (ratio * 0.5) * p;
		List<Long> s = new ArrayList<>();
		List<Long> l = new ArrayList<>();
		for (int i = 0; i < listKs.size(); i++) {
			if (listKs.get(i) < splitValue) {
				s.add(listKs.get(i));
			} else {
				l.add(listKs.get(i));
			}
		}

		// return s as the answer is l is empty
		if (l.isEmpty()) {
			subset = s;
		} else {
			l.forEach(i -> {
				// set the quantized value if c_i
				int v = (int) Math.ceil(i / d);
				for (int j = g - v; j >= 0; j--) {
					if (x[j] != -1) {
						if (x[j + v] < x[j] + i) {
							buckets[j + v] = new ArrayList(buckets[j]);
//							if (!buckets[j + v].contains(i)) {
								buckets[j + v].add(i);
//							}
							x[j + v] = x[j] + i;
						}
					}
				}
			});

			for (int i = 0; i < buckets.length; i++) {
				if (x[i] != -1) {
					int j = 0;
					while (x[i] < target && j < s.size()) {
						buckets[i].add(s.get(j));
						x[i] += s.get(j);
						j++;
					}
				}
			}
			int position = 0;
			long min = Integer.MAX_VALUE;
			for (int j = 0; j <= g; j++) {
				if (x[j] >= target && x[j] < min) {
					min = x[j];
					position = j;
				}
			}
			subset = buckets[position];
		}
		return subset;
	}
}
