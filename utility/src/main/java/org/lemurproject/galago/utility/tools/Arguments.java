package org.lemurproject.galago.utility.tools;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.json.JSONUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author jfoley, irmarc, sjh, trevor
 */
public class Arguments {
	public static Parameters parse(String[] args) throws IOException {
		Parameters self = Parameters.create();

		for (String arg : args) {
			if (arg.startsWith("--")) {
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

	protected static void tokenizeComplexValue(Parameters map, String pattern) throws IOException {
		int eqPos = pattern.indexOf('=') == -1 ? Integer.MAX_VALUE : pattern.indexOf('=');
		int arPos = pattern.indexOf('/') == -1 ? Integer.MAX_VALUE : pattern.indexOf('/');
		int plPos = pattern.indexOf('+') == -1 ? Integer.MAX_VALUE : pattern.indexOf('+');

		int smallest = (eqPos < arPos) ? (eqPos < plPos ? eqPos : plPos) : (arPos < plPos ? arPos : plPos);
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

	private static void tokenizeSimpleValue(Parameters map, String key, String value, boolean isArray) throws IOException {
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
