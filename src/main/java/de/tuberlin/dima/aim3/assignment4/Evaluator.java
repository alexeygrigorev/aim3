/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter, Christoph Boden
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.tuberlin.dima.aim3.assignment4;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class Evaluator {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSource<String> predictions = env.readTextFile(Config.pathToOutput());
		DataSet<Tuple3<String, String, Double>> classifiedDataPoints = predictions
				.map(new ClassificationResultReader());
		
		DataSet<String> evaluation = classifiedDataPoints.reduceGroup(new Evaluate());
		evaluation.print();

		env.execute();
	}

	public static class ClassificationResultReader implements
			MapFunction<String, Tuple3<String, String, Double>> {
		@Override
		public Tuple3<String, String, Double> map(String line) throws Exception {
			String[] elements = line.split("\t");
			String actualLabel = elements[0];
			String predictedLabel = elements[1];
			double score = Double.parseDouble(elements[2]);
			return new Tuple3<String, String, Double>(actualLabel, predictedLabel, score);
		}
	}

	public static class Evaluate implements GroupReduceFunction<Tuple3<String, String, Double>, String> {
		private long correct = 0L;
		private long total = 0L;

		@Override
		public void reduce(Iterable<Tuple3<String, String, Double>> predictions, Collector<String> collector)
				throws Exception {
			for (Tuple3<String, String, Double> tuple : predictions) {
				if (ObjectUtils.equals(tuple.f0, tuple.f1)) {
					correct++;
				}
				total++;
			}

			double accuracy = ((double) correct) / total;
			collector.collect("Classifier achieved: " + accuracy + " % accuracy");
		}
	}

}
