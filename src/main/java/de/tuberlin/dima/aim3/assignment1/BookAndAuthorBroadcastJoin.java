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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import de.tuberlin.dima.aim3.HadoopJob;

public class BookAndAuthorBroadcastJoin extends HadoopJob {

	@Override
	public int run(String[] args) throws Exception {
		Map<String, String> parsedArgs = parseArgs(args);

		Path authors = new Path(parsedArgs.get("--authors"));
		Path books = new Path(parsedArgs.get("--books"));
		Path outputPath = new Path(parsedArgs.get("--output"));

		Job job = prepareJob(books, outputPath, TextInputFormat.class,
				BroadcastJoinMapper.class, Text.class, Text.class,
				TextOutputFormat.class);
		
		Configuration conf = job.getConfiguration();
		DistributedCache.addCacheFile(authors.toUri(), conf);

		job.waitForCompletion(true);
		return 0;
	}

	static class BroadcastJoinMapper extends Mapper<Object, Text, Text, Text> {
		
		private final Map<String, String> index = new HashMap<String, String>();

		@Override
		protected void setup(Context ctx) throws IOException,
				InterruptedException {
			super.setup(ctx);
			Configuration config = ctx.getConfiguration();
			FileSystem fs = FileSystem.get(config);
			
			Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(config);
			for (Path path : cacheFilesLocal) {
				processAuthors(fs, path);
			}
		}

		private void processAuthors(FileSystem fs, Path path) throws IOException {
			FSDataInputStream is = fs.open(path);
			List<String> lines = IOUtils.readLines(is);
			for (String line : lines) {
				String[] split = line.split("\t");
				index.put(split[0], split[1]);
			}
		}

		@Override
		protected void map(Object key, Text line, Context ctx) throws IOException, InterruptedException {
			String stringLine = line.toString();
			String[] split = stringLine.split("\t");
	
			String id = split[0];
			if (!index.containsKey(id)) {
				return;
			}

			String author = index.get(id);
			String bookTitle = split[2];
			String year = split[1];

			ctx.write(new Text(author), new Text(bookTitle + '\t' + year));
		}
	}

}