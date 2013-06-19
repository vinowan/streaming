package com.tencent.qqlive.streaming.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPInfo {
	public static class IPBlockInfo {
		public long beginIP;
		public long endIP;
		public String country;
		public String province;
		public String city;
		public String service;
		
		@Override
		public String toString() {
			String ret = "";
			
			try {
				ret = String.format("%s~%s\t%s\t%s\t%s\t%s", 
						Utils.longToInetAddr(beginIP).toString(),
						Utils.longToInetAddr(endIP).toString(),
						country, province, city, service);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return ret;
		}
	}
	
	private static final Logger logger = LoggerFactory.getLogger(IPInfo.class);
	
	private NavigableMap<Long, IPBlockInfo> ipBlocks = new TreeMap<Long, IPBlockInfo>();
	
	public boolean init(InputStream input) {
		ipBlocks.clear();
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(input));
			
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] fields = line.split("\\s+");
				if (fields.length < 5)
					continue;
				
				IPBlockInfo ipBlock = new IPBlockInfo();
				
				ipBlock.beginIP = Long.valueOf(fields[0]);
				ipBlock.endIP = Long.valueOf(fields[1]);
				ipBlock.service = fields[2];
				ipBlock.country = fields[3];
				ipBlock.province = fields[4];
				if (fields.length == 6)
					ipBlock.city = fields[5];
				
				ipBlocks.put(ipBlock.beginIP, ipBlock);
			}	
		} catch (Exception e) {
			logger.error("failed to load file: " + Utils.stringifyException(e));
			return false;
		}
		
		return true;
	}
	
	public IPBlockInfo getIPBlock(long ip) {
		System.out.println("long: " + ip);
		Map.Entry<Long, IPBlockInfo> entry = ipBlocks.floorEntry(ip);
		if (entry == null)
			return null;
		
		if (entry.getValue().endIP > ip)
			return entry.getValue();
		
		return null;
	}
	
	public IPBlockInfo getIPBlock(String ip) {
		InetAddress addr = null;
		try {
			addr = InetAddress.getByName(ip);
		} catch (UnknownHostException e) {
			logger.error("failed to get ip: " + Utils.stringifyException(e));
			return null;
		}
		
		long value = Utils.inetAddrToLong(addr);
		return getIPBlock(value);
	}
}
