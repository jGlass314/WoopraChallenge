package com.woopra.challenge.quickstart;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
	final static int batchSize = 5;
	final static int limit = 100;
	final static Map<Long, List<String>> timeMap = new TreeMap<>(Collections.reverseOrder());
	final static String tempDirectory = "resources/tmp";
	final static String inputFile = "resources/file.txt";
	
	public static void reduce() {
		final File folder = new File(tempDirectory);
		final Map<String, List<Long>> pidMap = new HashMap<>();
		for(File file : folder.listFiles()) {
			JSONParser parser = new JSONParser();
			try (BufferedReader b = new BufferedReader(new FileReader(file))) {
				String readLine = "";
				while ((readLine = b.readLine()) != null) {
					Object obj = parser.parse(readLine);
					JSONObject jsonPidTMax =  (JSONObject) obj;
	                String pid = (String) jsonPidTMax.get("pid");
	                Long maxTime = (Long) jsonPidTMax.get("time");
	                if(!pidMap.containsKey(pid)) {
	                	pidMap.put(pid, new ArrayList<Long>());
	                }
	                List<Long> timeList = pidMap.get(pid);
	                timeList.add(maxTime);
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		// all the max times are in pidMap. Now we must get the max of the max times
		// store in timeMap while we're at it...
		for(String pid : pidMap.keySet()) {
			Collections.sort(pidMap.get(pid), Collections.reverseOrder());
			Long maxTime = pidMap.get(pid).get(0);
			if(!timeMap.containsKey(maxTime)) {
				timeMap.put(maxTime, new ArrayList<String>());
			}
			List<String> pidList = timeMap.get(maxTime);
			pidList.add(pid);
		}
	}
	
	public static void printReport() {
		int entryCount = 0;
		for(Long maxTime : timeMap.keySet()) {
			List<String> pidList = timeMap.get(maxTime);
			for(String pid : pidList) {
				if(entryCount++ == limit) {
					return;
				}
				System.out.println(pid + " | " + maxTime);
			}
		}
	}
	
	@SuppressWarnings("unchecked")
    public static void main( String[] args )
    {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());;
        JSONParser parser = new JSONParser();

        try (BufferedReader b = new BufferedReader(new FileReader(inputFile))) {
        	Map<String, List<Long>> map = new HashMap<>();
        	String readLine = "";
        	int lineNum = 0;
        	
        	System.out.println("Setup temp directory...");
        	new File(tempDirectory).mkdirs();
        	
        	System.out.println("Parsing input and passing to mapper...");
        	while ((readLine = b.readLine()) != null) {
        		Object obj = parser.parse(readLine);
                JSONObject jsonVisitorObject =  (JSONObject) obj;
                String pid = (String) jsonVisitorObject.get("pid");
                if(!map.containsKey(pid)) {
                	map.put(pid, new ArrayList<Long>());
                }
                JSONArray actions = (JSONArray) jsonVisitorObject.get("actions");
				Iterator<JSONObject> iterator = actions.iterator();
                while (iterator.hasNext()) {
                	Long time = (Long) iterator.next().get("time");
                	List<Long> timeList = map.get(pid);
                	timeList.add(time);
                }
                if(++lineNum % batchSize == 0) {
                	// send map and filename to mapper
                	Runnable mapper = new MapperThread(map, tempDirectory + "/" + lineNum/batchSize + ".txt");
                	executor.execute(mapper);
                	map = new HashMap<String, List<Long>>();
                }
            }
        	// flush remaining entries in map to mapper thread
        	Runnable mapper = new MapperThread(map, tempDirectory + "/" + ((lineNum/batchSize) + 1) + ".txt");
        	executor.execute(mapper);
 
        	executor.shutdown();  
            while (!executor.isTerminated()) {}  
            System.out.println("Finished Mapping.");
            System.out.println("Calling Reducer...");
            // call reducer
            reduce();
            System.out.println("Finished Reducing.");
            System.out.println("Printing report:");
            printReport();
            System.out.println("System cleanup...");
            FileUtils.deleteDirectory(new File(tempDirectory));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
