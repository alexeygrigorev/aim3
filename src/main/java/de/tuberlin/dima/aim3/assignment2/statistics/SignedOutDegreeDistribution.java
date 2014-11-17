/**
 * Graph-Mining Tutorial for Ozone
 *
 * Copyright (C) 2013  Sebastian Schelter <ssc@apache.org>
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

package de.tuberlin.dima.aim3.assignment2.statistics;

import java.io.File;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;

import de.tuberlin.dima.aim3.assignment2.Config;

public class SignedOutDegreeDistribution {

	
	private ExecutionEnvironment env;
	private DataSet<Tuple3<Long, Long, Boolean>> edges;

	public void run() throws Exception {
		env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<String> input = env.readTextFile(Config.pathToSlashdotZoo());
		edges = input.flatMap(new OutDegreeDistribution.EdgeReader());

		calculateForClass(EdgeType.FRIEND);
		calculateForClass(EdgeType.FOE);
	}

	public void calculateForClass(EdgeType edgeType) throws Exception {
		FilterOperator<Tuple3<Long, Long, Boolean>> filtered = edges.filter(new EdgeTypeFilter(edgeType));

		DataSet<Long> numVertices = filtered.project(0).types(Long.class)
				.union(filtered.project(1).types(Long.class))
				.distinct()
				.reduceGroup(new OutDegreeDistribution.CountVertices());

		DataSet<Tuple2<Long, Long>> verticesWithDegree = 
				filtered.project(0).types(Long.class)
				.groupBy(0)
				.reduceGroup(new OutDegreeDistribution.DegreeOfVertex());

		DataSet<Tuple2<Long, Double>> degreeDistribution = verticesWithDegree.groupBy(1)
				.reduceGroup(new OutDegreeDistribution.DistributionElement())
				.withBroadcastSet(numVertices, "numVertices");

		degreeDistribution.writeAsText(fileName(edgeType), FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	public String fileName(EdgeType edgeType) {
		String outputPath = Config.outputPath();

		if (edgeType == EdgeType.FRIEND) {
			return new File(outputPath, "ex1-frieds").getAbsolutePath();
		} else {
			return new File(outputPath, "ex1-foes").getAbsolutePath();
		}
	}


	private static final class EdgeTypeFilter implements FilterFunction<Tuple3<Long, Long, Boolean>> {
		private final Boolean desiredValue;

		public EdgeTypeFilter(EdgeType type) {
			this.desiredValue = Boolean.valueOf(type.getType());
		}

		@Override
		public boolean filter(Tuple3<Long, Long, Boolean> value) throws Exception {
			return desiredValue.equals(value.getField(2));
		}
	}


	static enum EdgeType {
		FRIEND(true), FOE(false);
		
		private boolean type;

		private EdgeType(boolean type) {
			this.type = type;
		}
		
		public boolean getType() {
			return type;
		}
	}

	
	public static void main(String[] args) throws Exception {
		new SignedOutDegreeDistribution().run();
	}

}
