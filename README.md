# WAGen
A synthetic query-aware database generator.

Given a database schema definition D and a set of queries Q = {Q1,Q2,...,Q_n}, where every query Q_i is annotated
with a set of constraints Ci (e.g value distribution, cardinality constraint). Our solution generates m <= n databases DB_1,
DB_2,..., DB_m such that; 1) all databases DB_i (1 <= i <= m) conform D, and 2) the resulting cardinalities C'_i of posing Q_i
over one of the databases DB_j approximate satisfy C_i.

This project is based on MyBencharkm (E. Lo, C. Binnig, D. Kossmann, M. T. O¨ zsu, and W. Hon. A framework for testing DBMS features. VLDB J., 19(2):203–230, 2010).
