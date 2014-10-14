package org.cghr.cme25;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This class provides a wrapper for parsing diff.json, found in the file
 * physician/testcmerules.js in cme 2.5. It provides a method, isDdxOf(), which
 * will tells whether an icd code is a differential diagnosis of another.
 *
 */
public class DdxWrapper {
	private Map<String, Map<String, String>> ddxMap = new HashMap<String, Map<String, String>>();
	private static DdxWrapper ddxWrapper = null;

	private DdxWrapper() {
		try {
			FileReader diffJsonFileReader = new FileReader(new File(this.getClass().getResource("/diff.json").toURI()));

			JsonParser jsonParser = new JsonParser();
			JsonArray jsonArray = jsonParser.parse(diffJsonFileReader).getAsJsonArray();

			for (int i = 0; i < jsonArray.size(); i++) {
				JsonObject diffForIcdJsonObject = jsonArray.get(i).getAsJsonObject();
				String icd = diffForIcdJsonObject.get("icd").getAsString();

				JsonArray diffJsonArray = diffForIcdJsonObject.get("diffs").getAsJsonArray();

				Map<String, String> diffIcdList = new HashMap<String, String>();

				for (int j = 0; j < diffJsonArray.size(); j++) {
					JsonArray diffJson = diffJsonArray.get(j).getAsJsonArray();
					String icdRange = diffJson.get(0).getAsString();
					String description = diffJson.get(1).getAsString();

					diffIcdList.put(icdRange, description);
				}
				ddxMap.put(icd, diffIcdList);
			}
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static DdxWrapper getInstance() {
		if (ddxWrapper == null) {
			ddxWrapper = new DdxWrapper();
		}

		return ddxWrapper;
	}

	public boolean isDdxOf(String icd1, String icd2) {
		System.out.println("isDdxOf(" + icd1 + "," + icd2 + ")");
		Map<String, String> map = ddxMap.get(icd2);

		if (map == null) {
			return false;
		} else {
			for (String key : map.keySet()) {
				String[] discreteTokens = key.split(",");

				for (String token : discreteTokens) {
					String[] continuousTokens = token.split("-");

					if (continuousTokens.length == 1) {
						if(continuousTokens[0].equals(icd1)) {
							return true;
						}
					} else {
						if (continuousTokens[0].charAt(0) == icd1.charAt(0)) {
							String minString = continuousTokens[0].substring(1).trim();
							String maxString = continuousTokens[1].substring(1).trim();

							int min = Integer.parseInt(minString);
							int max = Integer.parseInt(maxString);

							String icdNumString = icd1.substring(1);
							int icdNum = Integer.parseInt(icdNumString);

							if (icdNum >= min && icdNum <= max) {
								return true;
							}
						}
					}
				}
			}

			return false;
		}
	}

	public static void main(String[] args) {
		DdxWrapper ddxWrapper = DdxWrapper.getInstance();
		System.out.println(ddxWrapper.isDdxOf("J12", "A00"));
	}
}