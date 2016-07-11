/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.metron.parsers.windowssyslog;

import org.apache.metron.parsers.BasicParser;
import org.apache.metron.parsers.GrokParser;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class WindowsSyslogParser extends BasicParser {

	protected DateFormat dateFormat;

	protected int TIMEZONE_OFFSET;

	private static final long serialVersionUID = -535234013637774698L;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(WindowsSyslogParser.class);



	@Override
	public void configure(Map<String, Object> config) {
		withDateFormat((String) config.get("dateFormat"));
		Object timezoneOffset = config.get("timezoneOffset");
		if(timezoneOffset == null || ! (timezoneOffset instanceof Integer)) {
			withTimezoneOffset(0);
		}
		else {
			withTimezoneOffset((Integer)timezoneOffset);
		}
	}
	@Override
	public void init() {

	}

	public WindowsSyslogParser withDateFormat(String dateFormat) {
		if(dateFormat == null) {
			this.dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a", Locale.US);
		}
		else {
			this.dateFormat = new SimpleDateFormat(dateFormat, Locale.US);
		}
		this.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return this;
	}

	public WindowsSyslogParser withTimezoneOffset(Integer offset) {
		if(offset == null) {
			TIMEZONE_OFFSET = 0;
		}
		else {
			TIMEZONE_OFFSET = offset;
		}

		return this;
	}

	@Override
	public List<JSONObject> parse(byte[] rawMessage) {
		ArrayList<JSONObject> toReturn = new ArrayList<JSONObject>();
		toReturn.add(getParsedJSON(new String(rawMessage)));
		return toReturn;
	}

	private JSONObject getParsedJSON(String fileName) {
		JSONObject toReturn = new JSONObject();

		// if using test generator, read from file
		if (fileName.matches("^/opt/sampledata/windows-syslog/test-windows\\d+\\.log$")) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(fileName));
				String currentLine;
				StringBuffer sb = new StringBuffer();
				while ((currentLine = br.readLine()) != null) {
					sb.append(currentLine);
				}
				fileName = sb.toString();
				br.close();
			} catch (IOException e) {
				LOGGER.error("Unable to locate test file.", e);
				throw new IllegalStateException("Unable to find file: " + fileName + " due to " + e.getMessage(), e);
			}
		}
		try {
			ArrayList<String> results = getWindowsSyslogString(fileName);

			String windowsSyslogString = handleAndRemoveFirstLine(toReturn, results.get(0));
			String message = results.get(1);

			BufferedReader br = new BufferedReader(new StringReader(windowsSyslogString));
			String line;
			while ((line = br.readLine()) != null) {
				String []fields = line.split("=");
				String formattedKey = toStandardKeyConvention(fields[0]);
				if (fields.length < 2) {
					toReturn.put(formattedKey, "");
				} else {
					toReturn.put(formattedKey, fields[1]);
				}
			}

			String messageFull = results.get(1);

			String []fields = messageFull.split("\n");
			boolean logonType = false;
			boolean securityID = false;
			boolean subjectAccountName = false;
			boolean newLogonAccountName = false;
			boolean authPackage = false;
			boolean logonProcess = false;
			boolean sourceAddress = false;
			boolean keyLength = false;
			boolean packageName = false;
			for(int i = 0; i < fields.length; i++) {
				if (fields[i].contains("Logon Type")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("logon_type", value.replaceAll("\\s+",""));
					logonType = true;
				}

				if (fields[i].contains("Security ID")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("security_id", value.replaceAll("\t",""));
					securityID = true;
				}

				if (fields[i].contains("Authentication Package")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("authentication_package", value.replaceAll("\t",""));
					authPackage = true;
				}

				if (fields[i].contains("Source Network Address")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("source_address", value.replaceAll("\t",""));
					sourceAddress = true;
				}

				if (fields[i].contains("Package Name")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("package_name_ntlm", value.replaceAll("\t",""));
					packageName = true;
				}
				if (fields[i].contains("Key Length")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("key_length", value.replaceAll("\t",""));
					keyLength = true;
				}

				if (fields[i].contains("Logon Process")) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("logon_process", value.replaceAll("\t",""));
					logonProcess = true;
				}

				if (fields[i].contains("Account Name") && subjectAccountName==true && newLogonAccountName==false) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("newLogonAccountName", value.replaceAll("\\s+",""));
					newLogonAccountName = true;
				}

				if (fields[i].contains("Account Name") && subjectAccountName==false) {
					String value = "";
					value = fields[i].substring(fields[i].indexOf(":")+1,fields[i].length());
					toReturn.put("subjectAccountName", value.replaceAll("\\s+",""));
					subjectAccountName = true;
				}
			}
			if(logonType == false) {
				toReturn.put("logon_type", "");
			}
			if(securityID == false) {
				toReturn.put("security_id", "");
			}
			if(authPackage == false) {
				toReturn.put("authentication_package", "");
			}
			if(logonProcess == false) {
				toReturn.put("logon_process", "");
			}
			if(sourceAddress == false) {
				toReturn.put("source_address","");
			}
			if(packageName == false) {
				toReturn.put("package_name","");
			}
			if(keyLength == false) {
				toReturn.put("key_length","");
			}
			if(subjectAccountName == false) {
				toReturn.put("subjectAccountName", "");
			}
			if(newLogonAccountName == false) {
				toReturn.put("newLogonAccountName", "");
			}
		}
		catch (ParseException e) {
			LOGGER.error("Unable to parse timestamp in first line of windows syslog.", e);
			throw new IllegalStateException("Unable to parse timestamp in first line of windows syslog message: " + fileName + "\n due to " + e.getMessage(), e);
		} catch (IOException | IndexOutOfBoundsException e) {
			LOGGER.error("Unable to properly read windows syslog file.", e);
			throw new IllegalStateException("Unable to properly read windows syslog file: " + fileName + "\n due to " + e.getMessage(), e);
		}
		if (null == toReturn) {
			throw new IllegalStateException("Unable to Parse Windows Syslog message: " + fileName);
		}

		cleanJSON(toReturn, "Windows Syslog");

		return toReturn;
	}

	/**
	 * Adds the current timestamp so we know when the file was ingested
	 * @param parsedJSON the json that the parser created
	 */
	private void addIngestTimestamp(JSONObject parsedJSON){
		parsedJSON.put("ingest_timestamp", System.currentTimeMillis());
	}

	/**
	 * Adds the source type of the log
	 * @param parsedJSON the json that the parser created
	 * @param sourceType The source type of the log
	 */
	private void addSourceType(JSONObject parsedJSON, String sourceType) {
		parsedJSON.put("source_type", sourceType);
	}

	/**
	 * Cleans the json created by the parser
	 * @param parsedJSON the json that the parser created
	 * @param sourceType The source type of the log
	 */
	protected void cleanJSON(JSONObject parsedJSON, String sourceType) {
		removeEmptyAndNullKeys(parsedJSON);
		removeUnwantedKey(parsedJSON);
		//addIngestTimestamp(parsedJSON);
		timestampCheck(sourceType, parsedJSON);
		//addSourceType(parsedJSON, sourceType);
	}


	/**
	 * Removes the 'UNWANTED' key from the json
	 * @param parsedJSON the json the parser created
	 */
	private void removeUnwantedKey(JSONObject parsedJSON) {
		parsedJSON.remove("UNWANTED");

	}

	/**
	 * Removes empty and null keys from the json
	 * @param parsedJSON the json the parser created
	 */
	private void removeEmptyAndNullKeys(JSONObject parsedJSON) {
		Iterator<Object> keyIter = parsedJSON.keySet().iterator();
		while (keyIter.hasNext()) {
			Object key = keyIter.next();
			Object value = parsedJSON.get(key);
			// if the value is null or an empty string, remove that key.
			if (null == value || "".equals(value.toString())) {
				keyIter.remove();
			}
		}
	}

	/**
	 * Checks if a timestamp key exists. If it does not, it creates one.
	 * @param parsedJSON the json the parser created
	 */
	private void timestampCheck(String sourceType, JSONObject parsedJSON) {
		if (!parsedJSON.containsKey("timestamp")) {
			parsedJSON.put("timestamp", System.currentTimeMillis());
			//parsedJSON.put("device_generated_timestamp", parsedJSON.get("timestamp"));
		}
		else {
			if (parsedJSON.get("timestamp") instanceof String){
				long longTimestamp = 0;
				try {
					longTimestamp = Long.parseLong( (String) parsedJSON.get("timestamp"));
				} catch (NumberFormatException e) {
					LOGGER.error("Unable to parse a long from the timestamp field.", e);
				}
				parsedJSON.put("timestamp", longTimestamp);
			}
			convertTimezoneToUTC(sourceType, parsedJSON);
		}
	}

	/**
	 * Checks if a timestamp key exists. If it does not, it creates one.
	 * Converts the timezone to UTC based on the value in the timezone map
	 * @param parsedJSON the json the parser created
	 */
	private void convertTimezoneToUTC(String sourceType, JSONObject parsedJSON) {
		parsedJSON.put("device_generated_timestamp", parsedJSON.get("timestamp"));
		long newTimestamp = (long) parsedJSON.get("timestamp");
		if (TIMEZONE_OFFSET != 24) {
			newTimestamp = newTimestamp + (TIMEZONE_OFFSET * 3600000);
			parsedJSON.put("timestamp", newTimestamp);
		}
		else {
			long timeDifference = (long) parsedJSON.get("ingest_timestamp") - (long) parsedJSON.get("device_generated_timestamp");
			long estimateOffset = timeDifference/3600000;
			newTimestamp = newTimestamp + (estimateOffset * 3600000);
			parsedJSON.put("timestamp", newTimestamp);
		}
	}

	private static ArrayList<String> getWindowsSyslogString(String fileName) throws IOException {
		BufferedReader br = null;
		StringBuilder windowsSyslogExceptMessage = new StringBuilder();
		StringBuilder windowsSyslogMessage = new StringBuilder();
		boolean hasHitMessage = false;

		String currentLine;
		br = new BufferedReader(new StringReader(fileName));
		int counter = 0;
		while (null != ((currentLine = br.readLine()))) {
			LOGGER.info("Processing line: " + currentLine);
			// if the currentLine is not empty or just a new line character
			if (!("".equals(currentLine) || "\n".equals(currentLine))) {
				if (!hasHitMessage && currentLine.contains("Message=")) {
					hasHitMessage = true;
				}
				if (!hasHitMessage) {
					appendWithNewLine(windowsSyslogExceptMessage, currentLine);
				} else {
					appendWithNewLine(windowsSyslogMessage, currentLine);
				}
			}
		}
		if (null != br) {
			br.close();
		}
		ArrayList<String> toReturn = new ArrayList<String>();
		toReturn.add(0, windowsSyslogExceptMessage.toString());
		toReturn.add(1, windowsSyslogMessage.toString());
		return toReturn;
	}

	/**
	 * Handles the first line in a Windows Syslog file, adding the appropriate fields to the provided json.
	 * This method also returns fileName but with the first line removed.
	 * The first line of a windows syslog file should be ion the following format:
	 * <<number>> <SimpleComputerName> MM/dd/yyyy hh:mm:ss <AM|PM>
	 * The number is likely an unneeded priority. SimpleComputerName is the name of the computer without its domain name.
	 * The rest is the timestamp of the windows syslog, divided into 3 parts.
	 * @param json The JSONObject that will contain the mappings from the Windows Syslog file.
	 * @param fileName The Windows Syslog file to be parsed.
	 * @return fileName but with the first line removed.
	 * @throws ParseException
	 */
	private String handleAndRemoveFirstLine(JSONObject json, String fileName) throws ParseException {
		int indexOfFirstNewLine = fileName.indexOf('\n');

		String firstLine = fileName.substring(0, indexOfFirstNewLine);
		String[] splitFirstLine = firstLine.split(" ");

		String timestamp = getTimestampAsSingleString(splitFirstLine);

		json.put("computer_name_simple", splitFirstLine[1]);

		json.put("timestamp", getEpochTime(timestamp));
		return fileName.substring(indexOfFirstNewLine + 1);
	}

	/**
	 * Handles pulling out the timestamp from the first line of a windows syslog
	 * @param splitFirstLine The individual parts of the first line of the windows syslog
	 * @return The timestamp, formatted into a single string.
	 */
	private String getTimestampAsSingleString(String[] splitFirstLine) {
		StringBuilder sb = new StringBuilder();
		if (splitFirstLine.length > 2 && splitFirstLine[2].matches("\\d{2}/\\d{2}/\\d{4}")) {
			appendWithSpace(sb, splitFirstLine[2]);
		}
		if (splitFirstLine.length > 3 && splitFirstLine[3].matches("\\d{2}:\\d{2}:\\d{2}")) {
			appendWithSpace(sb, splitFirstLine[3]);
		}
		if (splitFirstLine.length > 4 && splitFirstLine[4].matches("AM|PM")) {
			sb.append(splitFirstLine[4]);
		}
		return sb.toString().trim(); // use trim in case any part of the timestamp is missing, possibly leading to trailing whitespaces.
	}

	private long getEpochTime(String timestamp) throws ParseException {
		Date date = dateFormat.parse(timestamp);
		return date.getTime();
	}

	private static void appendWithNewLine(StringBuilder sb, String toAppend) {
		sb.append(toAppend + "\n");
	}

	private static void appendWithSpace(StringBuilder sb, String toAppend) {
		sb.append(toAppend + " ");
	}

	private String toStandardKeyConvention(String keyIn) {
		StringBuilder sb = new StringBuilder();
		sb.append(Character.toLowerCase(keyIn.charAt(0)));
		for (int i = 1; i < keyIn.length(); i++) {
			char current = keyIn.charAt(i);
			if (Character.isUpperCase(current)) {
				sb.append('_');
			}
			sb.append(Character.toLowerCase(current));
		}
		return sb.toString();
	}


}
