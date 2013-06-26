package com.tencent.qqlive.streaming.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ComponentStats implements Runnable {
	protected static final Logger logger = LoggerFactory
			.getLogger(ComponentStats.class);
	
	// ���������
	public abstract String getComponentName();
	
	// ������־�ַ���
	public abstract String toStr();
	
	// ���ü�����
	public abstract void reset();
	
	public void run() {
		logger.info("\n" + getComponentName() + " stats: \n" + toStr());
		reset();
	}

}
