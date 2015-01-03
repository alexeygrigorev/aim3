package de.tuberlin.dima.aim3.assignment1;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

public class Utils {

	public static final Set<String> STOP_WORDS;

	static {
		List<String> stopWords = Arrays.asList("a", "about", "above", "after", "again", "against", "am",
				"an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below",
				"between", "both", "but", "by", "cannot", "could", "did", "do", "does", "doing", "down",
				"during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he",
				"her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in", "into",
				"is", "it", "its", "itself", "me", "more", "most", "my", "myself", "no", "nor", "not", "of",
				"off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
				"over", "own", "same", "she", "should", "so", "some", "such", "than", "that", "the", "their",
				"theirs", "them", "themselves", "then", "there", "these", "they", "this", "those", "through",
				"to", "too", "under", "until", "up", "very", "was", "we", "were", "what", "when", "where",
				"which", "while", "who", "whom", "why", "with", "would", "you", "your", "yours", "yourself",
				"yourselves");
		STOP_WORDS = ImmutableSet.copyOf(stopWords);
	}

	public static List<String> extractTokens(String contents) {
		List<String> result = Lists.newArrayList();
		String[] tokens = contents.split("(\\s+|[,.?!;])");
		for (int i = 0; i < tokens.length; i++) {
			String token = tokens[i];
			if (nonEmptyAlphanumeric(token) && notStopWord(token)) {
				result.add(token);
			}
		}
		return result;
	}

	public static boolean notStopWord(String token) {
		return !STOP_WORDS.contains(token);
	}

	public static boolean nonEmptyAlphanumeric(String token) {
		return StringUtils.isNotEmpty(token) && StringUtils.isAlphanumeric(token);
	}

	public static int intCompareTo(int a, int b) {
		if (a < b) {
			return -1;
		} else if (a > b) {
			return 1;
		} else {
			return 0;
		}
	}
}
