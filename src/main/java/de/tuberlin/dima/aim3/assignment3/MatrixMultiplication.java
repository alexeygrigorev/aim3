package de.tuberlin.dima.aim3.assignment3;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;

import com.google.common.collect.ArrayTable;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.collect.Table;

public class MatrixMultiplication {

	public void run() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple3<Integer, Integer, Integer>> matrixA = readSparseMatrix(env, Config.MATRIX_A);
		DataSource<Tuple3<Integer, Integer, Integer>> matrixB = readSparseMatrix(env, Config.MATRIX_B);

		matrixA.join(matrixB).where(1).equalTo(0)
				.map(new ProjectJoinResultMapper()).groupBy(0, 1).sum(2)
				.writeAsCsv(Config.OUTPUT_PATH, WriteMode.OVERWRITE);

		env.execute();

		testOutput();
	}

	private void testOutput() throws IOException {
		Table<Integer, Integer, Integer> actual = readActual();
		Table<Integer, Integer, Integer> expected = readExpected();

		if (actual.equals(expected)) {
			System.out.println("We're cool");
		} else {
			throw new AssertionError("We're not cool");
		}
	}

	public Table<Integer, Integer, Integer> readExpected() throws IOException {
		Table<Integer, Integer, Integer> expected = createMatrix();
		List<String> lines = FileUtils.readLines(new File(Config.MATRIX_AB));
		for (String line : lines) {
			String[] split = line.split(",");
			expected.put(Integer.parseInt(split[1]), Integer.parseInt(split[2]), Integer.parseInt(split[3]));
		}
		return expected;
	}

	public Table<Integer, Integer, Integer> readActual() throws IOException {
		Collection<File> files = FileUtils.listFiles(new File(Config.OUTPUT_PATH), null, false);
		Table<Integer, Integer, Integer> actual = createMatrix();

		for (File file : files) {
			List<String> lines = FileUtils.readLines(file);
			for (String line : lines) {
				String[] split = line.split(",");
				actual.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]), Integer.parseInt(split[2]));
			}
		}

		return actual;
	}

	public static Table<Integer, Integer, Integer> createMatrix() {
		Iterable<Integer> keys = ContiguousSet.create(Range.closed(0, 99), DiscreteDomain.integers());
		return ArrayTable.create(keys, keys);
	}

	public DataSource<Tuple3<Integer, Integer, Integer>> readSparseMatrix(ExecutionEnvironment env,
			String filePath) {
		CsvReader csvReader = env.readCsvFile(filePath);
		csvReader.fieldDelimiter(',');
		csvReader.includeFields("fttt");
		return csvReader.types(Integer.class, Integer.class, Integer.class);
	}

	private static final class ProjectJoinResultMapper implements
				MapFunction<Tuple2<Tuple3<Integer, Integer, Integer>, 
								   Tuple3<Integer, Integer, Integer>>, 
						    Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> map(
				Tuple2<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> value)
				throws Exception {
			Integer row = value.f0.f0;
			Integer column = value.f1.f1;
			Integer product = value.f0.f2 * value.f1.f2;
			return new Tuple3<Integer, Integer, Integer>(row, column, product);
		}
	}
	
	public static void main(String[] args) throws Exception {
		new MatrixMultiplication().run();
	}

}
