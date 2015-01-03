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

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

public class Classification {

	public static void main(String[] args) throws Exception {
		String pathToTestSet = Config.pathToTestSet();
		int smoothing = Config.getSmoothingParameter();
		String pathToOutput = Config.pathToOutput();

		run(pathToTestSet, pathToOutput, smoothing);
	}

	public static void run(String testSetPath, String outputPath, int smoothing) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, String, Long>> conditionals = env.readTextFile(Config.pathToConditionals())
				.map(new ConditionalReader());
		DataSet<Tuple2<String, Long>> sums = env.readTextFile(Config.pathToSums())
				.map(new SumReader());
		DataSet<Tuple2<String, Double>> priors = env.readTextFile(Config.pathToPriors())
				.map(new PriorsReader());

		DataSource<String> testData = env.readTextFile(testSetPath);

		DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData
				.map(new Classifier(smoothing))
				.withBroadcastSet(conditionals, "conditionals")
				.withBroadcastSet(priors, "priors")
				.withBroadcastSet(sums, "sums");

		classifiedDataPoints.writeAsCsv(outputPath, "\n", "\t", FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {
		@Override
		public Tuple3<String, String, Long> map(String s) throws Exception {
			String[] elements = s.split("\t");
			String group = elements[0];
			String word = elements[1];
			long count = Long.parseLong(elements[2]);
			return new Tuple3<String, String, Long>(group, word, count);
		}
	}

	public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {
		@Override
		public Tuple2<String, Long> map(String s) throws Exception {
			String[] elements = s.split("\t");
			String group = elements[0];
			long count = Long.parseLong(elements[1]);
			return new Tuple2<String, Long>(group, count);
		}
	}

	public static class PriorsReader implements MapFunction<String, Tuple2<String, Double>> {
		@Override
		public Tuple2<String, Double> map(String s) throws Exception {
			String[] elements = s.split("\t");
			String group = elements[0];
			double probability = Double.parseDouble(elements[1]);
			return new Tuple2<String, Double>(group, probability);
		}
	}

	public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>> {
		private int smoothing;

		private final Map<String, Double> priors = Maps.newHashMap();
		private final Map<String, Long> wordSums = Maps.newHashMap();
		private final Table<String, String, Long> likelihoods = HashBasedTable.create();

		private final List<String> labels = Lists.newArrayList();
		private long distinctWordCount = 0;

		public Classifier(int smoothing) {
			this.smoothing = smoothing;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			readPriors();
			readConditionals();
			readWordSums();
		}

		private void readPriors() {
			RuntimeContext ctx = getRuntimeContext();
			List<Tuple2<String, Double>> priorsList = ctx.getBroadcastVariable("priors");
			for (Tuple2<String, Double> tuple : priorsList) {
				priors.put(tuple.f0, Math.log(tuple.f1));
			}
		}

		private void readConditionals() {
			RuntimeContext ctx = getRuntimeContext();
			List<Tuple3<String, String, Long>> conditionals = ctx.getBroadcastVariable("conditionals");
			for (Tuple3<String, String, Long> tuple : conditionals) {
				likelihoods.put(tuple.f0, tuple.f1, tuple.f2);
			}

			labels.addAll(likelihoods.rowKeySet());
			distinctWordCount = likelihoods.columnKeySet().size();
		}

		private void readWordSums() {
			RuntimeContext ctx = getRuntimeContext();
			List<Tuple2<String, Long>> sums = ctx.getBroadcastVariable("sums");
			for (Tuple2<String, Long> tuple : sums) {
				wordSums.put(tuple.f0, tuple.f1);
			}
		}

		@Override
		public Tuple3<String, String, Double> map(String line) throws Exception {
			String[] tokens = line.split("\t");
			String actualLabel = tokens[0];
			String[] terms = tokens[1].split(",");

			double maxLog = Double.NEGATIVE_INFINITY;
			String predictionLabel = "";

			for (String label : labels) {
				double logProb = calculateLogP(label, terms);
				if (logProb > maxLog) {
					maxLog = logProb;
					predictionLabel = label;
				}
			}

			return new Tuple3<String, String, Double>(actualLabel, predictionLabel, maxLog);
		}

		public double calculateLogP(String label, String[] terms) {
			double logProb = priors.get(label);
			Map<String, Long> countsPerLabel = likelihoods.row(label);

			// numerator terms
			for (String term : terms) {
				Long count = countsPerLabel.get(term);
				if (count != null) {
					logProb = logProb + Math.log(count + smoothing);
				} else {
					logProb = logProb + Math.log(smoothing);
				}
			}

			// denominator terms
			double denom = wordSums.get(label) + smoothing * distinctWordCount;
			logProb = logProb - terms.length * Math.log(denom);
			return logProb;
		}
	}

}
