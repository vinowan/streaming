package com.tencent.qqlive.streaming.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ComponentStats implements Runnable {
	protected static final Logger logger = LoggerFactory
			.getLogger(ComponentStats.class);
	
	// 返回组件名
	public abstract String getComponentName();
	
	// 返回日志字符串
	public abstract String toStr();
	
	// 重置计数器
	public abstract void reset();
	
	public void run() {
		logger.info("\n" + getComponentName() + " stats: \n" + toStr());
		reset();
	}

}
