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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Lists;

import de.tuberlin.dima.aim3.HadoopJob;

public class BookAndAuthorReduceSideJoin extends HadoopJob {

	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> parsedArgs = parseArgs(args);
		Path authors = new Path(parsedArgs.get("--authors"));
		Path books = new Path(parsedArgs.get("--books"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		Configuration conf = getConf();
		conf.set("hadoop.tmp.dir", "c:/tmp/");
		conf.set("fs.file.impl", "com.conga.services.hadoop.patch.HADOOP_7682.WinLocalFileSystem");
		Job job = new Job(new Configuration(conf));
		Configuration jobConf = job.getConfiguration();

		job.setJarByClass(BookAndAuthorReduceSideJoin.class);
		jobConf.setBoolean("mapred.compress.map.output", true);


		MultipleInputs.addInputPath(job, authors, TextInputFormat.class, AuthorMapper.class);
		MultipleInputs.addInputPath(job, books, TextInputFormat.class, BookMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(AuthorBookJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setJobName("book-author-reduceside-join");

		job.setOutputFormatClass(TextOutputFormat.class);
		jobConf.set("mapred.output.dir", outputPath.toString());

		job.waitForCompletion(true);

		return 0;
	}

	static class AuthorMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
			String stringLine = line.toString();
			String[] split = stringLine.split("\t");

			String joinKey = split[0];
			String author = split[1];

			ctx.write(new Text(joinKey), new Text("#AUTHOR#\t" + author));
		}
	}

	static class BookMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
			String stringLine = line.toString();
			String[] split = stringLine.split("\t");

			String joinKey = split[0];
			String bookTitle = split[2];
			String year = split[1];

			ctx.write(new Text(joinKey), new Text("#BOOK#\t" + bookTitle + '\t' + year));
		}
	}

	static class AuthorBookJoinReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException,
				InterruptedException {
			// assuming there's always only one author but many books
			String author = null;
			List<String[]> books = Lists.newArrayList();

			for (Text value : values) {
				String[] split = value.toString().split("\t");
				if ("#BOOK#".equals(split[0])) {
					books.add(split);
				} else if ("#AUTHOR#".equals(split[0])) {
					author = split[1];
				} else {
					throw new IllegalArgumentException("got unexpected tag " + split[0]);
				}
			}

			Validate.notNull(author, "no author found");

			for (String[] book : books) {
				ctx.write(new Text(author), new Text(book[1] + '\t' + book[2]));
			}
		}
	}
}