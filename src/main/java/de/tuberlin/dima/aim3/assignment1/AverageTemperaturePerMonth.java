/**
 * AIM3 - Scalable Data Mining -  course work
 * Copyright (C) 2014  Sebastian Schelter
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

package de.tuberlin.dima.aim3.assignment1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.tuberlin.dima.aim3.HadoopJob;

public class AverageTemperaturePerMonth extends HadoopJob {

	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> parsedArgs = parseArgs(args);

		Path inputPath = new Path(parsedArgs.get("--input"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		Job job = prepareJob(inputPath, outputPath, TextInputFormat.class,
				AverageTemperaturePerMonthMapper.class, YearMonthWritable.class, DoubleWritable.class,
				AverageTemperaturePerMonthReducer.class, Text.class, DoubleWritable.class,
				TextOutputFormat.class);

		double minimumQuality = Double.parseDouble(parsedArgs.get("--minimumQuality"));
		Configuration jobConfig = job.getConfiguration();
		jobConfig.setFloat("min.quality", (float) minimumQuality);

		job.waitForCompletion(true);

		return 0;
	}

	static class AverageTemperaturePerMonthMapper extends
			Mapper<Object, Text, YearMonthWritable, DoubleWritable> {
		@Override
		protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
			Configuration configuration = ctx.getConfiguration();
			float minimumQuality = configuration.getFloat("min.quality", 0.0f);

			String content = line.toString();
			String[] split = content.split("\t");

			float quality = Float.parseFloat(split[3]);
			if (quality < minimumQuality) {
				return;
			}

			int year = Integer.parseInt(split[0]);
			int month = Integer.parseInt(split[1]);
			double temperature = Double.parseDouble(split[2]);

			ctx.write(new YearMonthWritable(year, month), new DoubleWritable(temperature));
		}
	}

	static class AverageTemperaturePerMonthReducer extends
			Reducer<YearMonthWritable, DoubleWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(YearMonthWritable key, Iterable<DoubleWritable> values, Context arg)
				throws IOException, InterruptedException {

			int cnt = 0;
			double sum = 0.0;
			for (DoubleWritable dw : values) {
				sum = sum + dw.get();
				cnt++;
			}

			double avg = 0.0;
			if (cnt != 0) {
				avg = sum / cnt;
			}

			arg.write(new Text(key.getYear() + "\t" + key.getMonth()), new DoubleWritable(avg));
		}

	}

	static class YearMonthWritable implements WritableComparable<YearMonthWritable> {

		private int year;
		private int month;

		public YearMonthWritable() {
		}

		public YearMonthWritable(int year, int month) {
			this.year = year;
			this.month = month;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.year = in.readInt();
			this.month = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(year);
			out.writeInt(month);
		}

		public int getYear() {
			return year;
		}

		public int getMonth() {
			return month;
		}

		@Override
		public int compareTo(YearMonthWritable o) {
			if (year == o.year) {
				return Utils.intCompareTo(month, o.month);
			}

			return Utils.intCompareTo(year, o.year);
		}
	}

}