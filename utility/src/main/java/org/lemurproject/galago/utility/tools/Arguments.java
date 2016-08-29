package org.lemurproject.galago.utility.tools;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.json.JSONUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author jfoley, irmarc, sjh, trevor
 */
public class Arguments {
	static char OpObjectEquals = '=';
	static char OpListPlus = '+';
	static char OpMapAccess = '/';

	static boolean looksLikeKey(String x) {
		return x.startsWith("--");
	}
	/** This function returns true iff the argument ends with equals or +, this allows the use of tab-complete.
	 *
	 * --file= /next/arg/is/the/path.something
	 * --input+ /first/input.trec
	 *
	 * @param arg the command-line argument
	 */
	static boolean endsWithJoinOperator(String arg) {
		if(arg.isEmpty()) return false;
		char c = arg.charAt(arg.length()-1);
		return c == OpObjectEquals || c == OpListPlus;
	}

	/** This function combines adjacent arguments if the first one ends with an operator and the next one does not start with "--"
	 *
	 * --file= /next/arg/is/the/path.something
	 * --input+ /first/input.trec
	 *
	 * @param args the command-line arguments
	 */
	static List<String> combineAdjacentIfReasonable(String[] args) {
		List<String> joinedArgs = new ArrayList<>();
		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			// if this one is a dangling key, and the next one is not a key, join them.
			if (looksLikeKey(arg) && endsWithJoinOperator(arg)&& i+1 < args.length && !looksLikeKey(args[i+1])) {
				joinedArgs.add(args[i]+args[i+1]);
				i++;
			} else {
				joinedArgs.add(args[i]);
			}
		}
		return joinedArgs;
	}

	public static Parameters parse(String[] args) throws IOException {
		Parameters self = Parameters.create();

		List<String> reasonableArgs = combineAdjacentIfReasonable(args);

		for (String arg : reasonableArgs) {
			if(arg.isEmpty()) {
				continue;
			}
			if (looksLikeKey(arg)) {
				String pattern = arg.substring(2);
				tokenizeComplexValue(self, pattern);
			} else {
				// We assume that the input is a file of JSON parameters
				Parameters other = Parameters.parseFile(new File(arg));
				self.copyFrom(other);
			}
		}

		return self;
	}

	static int indexOrMax(String x, char c) {
		int pos = x.indexOf(c);
		if(pos == -1) return Integer.MAX_VALUE;
		return pos;
	}

	static void tokenizeComplexValue(Parameters map, String pattern) throws IOException {
		int eqPos = indexOrMax(pattern, OpObjectEquals);
		int arPos = indexOrMax(pattern, OpMapAccess);
		int plPos = indexOrMax(pattern, OpListPlus);

		int smallest = Collections.min(Arrays.asList(eqPos, arPos, plPos));
		if (smallest == Integer.MAX_VALUE) {
			// Assume they meant 'true' for the key
			map.set(pattern, true);
		} else {
			if (eqPos == smallest) {
				tokenizeSimpleValue(map, pattern.substring(0, smallest), pattern.substring(smallest + 1, pattern.length()), false);
			} else if (plPos == smallest) {
				tokenizeSimpleValue(map, pattern.substring(0, smallest), pattern.substring(smallest + 1, pattern.length()), true);
			} else {
				String mapKey = pattern.substring(0, smallest);
				if (!map.isMap(mapKey)) {
					map.set(mapKey, Parameters.create());
				}
				tokenizeComplexValue(map.getMap(mapKey), pattern.substring(smallest + 1, pattern.length()));
			}
		}
	}

	static void tokenizeSimpleValue(Parameters map, String key, String value, boolean isArray) throws IOException {
		Object v = JSONUtil.parseString(value);

		if(v instanceof String) {
			// attempt to clean a string: 'string'
			if (value.startsWith("'") && value.endsWith("'") && value.length() > 1) {
				v = JSONUtil.parseString(value.substring(1, value.length() - 1));
			}
		}

		if (isArray) {
			if (!map.isList(key)) {
				map.put(key, new ArrayList());
			}
			map.getList(key, Object.class).add(v);
		} else {
			map.put(key, v);
		}
	}
}
