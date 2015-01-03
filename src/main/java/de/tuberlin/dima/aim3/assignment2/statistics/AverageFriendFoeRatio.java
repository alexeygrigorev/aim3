package de.tuberlin.dima.aim3.assignment2.statistics;

import java.io.File;
import java.util.Iterator;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

import de.tuberlin.dima.aim3.assignment2.Config;

public class AverageFriendFoeRatio {

	public void run() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		FlatMapOperator<String, Tuple3<Long, Long, Boolean>> edges = readSlashdotData(env);

		DataSet<Tuple1<Double>> ratios = edges.groupBy(0)
				.reduceGroup(new FriendFoeRatioReducer())
				.project(1).types(Double.class);

		ratios.map(new Add1Mapper())
			  .reduce(new SumAndCountReducer())
			  .map(new SumCountTupleToAverageMapper())
			  .writeAsText(ex2FilePath(), WriteMode.OVERWRITE);

		env.execute();
	}

	public static FlatMapOperator<String, Tuple3<Long, Long, Boolean>> readSlashdotData(
			ExecutionEnvironment env) {
		DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());
		return input.flatMap(new OutDegreeDistribution.EdgeReader());
	}

	private String ex2FilePath() {
		return new File(Config.outputPath(), "ex2").getAbsolutePath();
	}

	private static class Add1Mapper implements MapFunction<Tuple1<Double>, Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> map(Tuple1<Double> value) throws Exception {
			return new Tuple2<Long, Double>(1L, value.f0);
		}
	}

	private static class SumAndCountReducer extends RichReduceFunction<Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> reduce(Tuple2<Long, Double> value1, Tuple2<Long, Double> value2)
				throws Exception {
			return new Tuple2<Long, Double>(value1.f0 + value2.f0, value1.f1 + value2.f1);
		}
	}

	private static class SumCountTupleToAverageMapper implements MapFunction<Tuple2<Long, Double>, Double> {
		@Override
		public Double map(Tuple2<Long, Double> value) throws Exception {
			return value.f1 / value.f0;
		}
	}

	private static class FriendFoeRatioReducer implements
			GroupReduceFunction<Tuple3<Long, Long, Boolean>, Tuple2<Long, Double>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, Long, Boolean>> values, Collector<Tuple2<Long, Double>> out)
				throws Exception {
			Iterator<Tuple3<Long, Long, Boolean>> iterator = values.iterator();
			Tuple3<Long, Long, Boolean> first = iterator.next();

			Long vertexId = first.f0;
			long countFried = 0;
			long countFoe = 0;

			if (Boolean.TRUE.equals(first.getField(2))) {
				countFried++;
			} else {
				countFoe++;
			}

			while (iterator.hasNext()) {
				Tuple3<Long, Long, Boolean> next = iterator.next();
				if (Boolean.TRUE.equals(next.getField(2))) {
					countFried++;
				} else {
					countFoe++;
				}
			}

			if (countFried > 0 && countFoe > 0) {
				double ratio = (double) countFried / countFoe;
				out.collect(new Tuple2<Long, Double>(vertexId, ratio));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		new AverageFriendFoeRatio().run();
	}

}
