package com.woopra.challenge.quickstart;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONObject;

public class MapperThread implements Runnable {

	private Map<String, List<Long>> map;
	private String fileName;
	public MapperThread(Map<String, List<Long>> map, String fileName) {
		this.map = map;
		this.fileName = fileName;
	}

	public void run() {
//		System.out.println(Thread.currentThread().getName()+" (Start) filename = "+this.fileName); 
		Map<String, Long> reducedMap = createReducedMap();
		writeReducedMapToFile(reducedMap);
//		System.out.println(Thread.currentThread().getName()+" (End)");//prints thread name 
	}
	
	private Map<String, Long> createReducedMap() {
		Map<String, Long> reducedMap = new HashMap<String, Long>();
		for(String pid : this.map.keySet()) {
			ArrayList<Long> times = (ArrayList<Long>) this.map.get(pid);
			if(times.size() == 0) {
				continue;
			}
//			System.out.println(pid + " times: " + times.toString());
			Collections.sort(times, Collections.reverseOrder());
//			System.out.println(pid + " maxtime: " + times.get(0));
			reducedMap.put(pid, times.get(0));
		}
		return reducedMap;
	}
	
	@SuppressWarnings("unchecked")
	private void writeReducedMapToFile(Map<String, Long> reducedMap) {
//		System.out.println("writing to file: " + this.fileName);
		try (BufferedWriter file = new BufferedWriter(new FileWriter(this.fileName))) {
			for(String pid : reducedMap.keySet()) {
				JSONObject obj = new JSONObject();
				obj.put("pid", pid);
				obj.put("time", reducedMap.get(pid));
				file.write(obj.toJSONString());
				file.write("\n");
			}
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }	
	}
}
