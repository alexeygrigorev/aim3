package de.tuberlin.dima.aim3.assignment3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

import de.tuberlin.dima.aim3.assignment3.MatrixMultiplication.ProjectJoinResultMapper;

public class Trace {

	public void run() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<Tuple3<Integer, Integer, Integer>> matrixA = MatrixMultiplication.readSparseMatrix(env,
				Config.MATRIX_A);
		DataSource<Tuple3<Integer, Integer, Integer>> matrixB = MatrixMultiplication.readSparseMatrix(env,
				Config.MATRIX_B);

		trace(multiply(matrixA, matrixB)).print();
		trace(multiply(matrixB, matrixA)).print();

		env.execute();
	}

	private DataSet<Tuple3<Integer, Integer, Integer>> trace(DataSet<Tuple3<Integer, Integer, Integer>> matrix) {
		return matrix.filter(new DiagonalElementsFilter()).sum(2);
	}

	private AggregateOperator<Tuple3<Integer, Integer, Integer>> multiply(DataSource<Tuple3<Integer, Integer, Integer>> matrixA,
			DataSource<Tuple3<Integer, Integer, Integer>> matrixB) {
		return matrixA.join(matrixB).where(1).equalTo(0)
				.map(new ProjectJoinResultMapper())
				.groupBy(0, 1).sum(2);
	}

	public static final class DiagonalElementsFilter implements
			FilterFunction<Tuple3<Integer, Integer, Integer>> {
		@Override
		public boolean filter(Tuple3<Integer, Integer, Integer> value) throws Exception {
			return value.f0.equals(value.f1);
		}
	}

	public static void main(String[] args) throws Exception {
		new Trace().run();
	}

}
