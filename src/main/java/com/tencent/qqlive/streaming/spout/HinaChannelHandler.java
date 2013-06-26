package com.tencent.qqlive.streaming.spout;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HinaChannelHandler extends SimpleChannelUpstreamHandler {
	private static final Logger logger = LoggerFactory.getLogger(HinaChannelHandler.class);
	
	private static final byte SPEARATOR = (byte) 0xe0;
	private static String EVENT_KEY_NAME = "category";
	
	private BlockingQueue<HinaEvent> messageQueue = null;
	
	private SpoutStatics statics = null;
	
	public HinaChannelHandler(BlockingQueue<HinaEvent> messageQueue, SpoutStatics statics) {
		this.messageQueue = messageQueue;
		this.statics = statics;
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		statics.inPacket.getAndIncrement();
		
		byte[] rawMessage = ((ChannelBuffer) e.getMessage()).array();
		
		int bodyIndex = searchIndex(rawMessage, SPEARATOR);
		if (bodyIndex <= 0) {
			bodyIndex = rawMessage.length - 1;
		}
		
		byte[] eventByte = Arrays.copyOfRange(rawMessage, 0, bodyIndex);		
		byte[] propertyByte = Arrays.copyOfRange(rawMessage, bodyIndex + 1,
				rawMessage.length - 1);
		
		String category = getEventCategory(propertyByte);
		if (category == null) {
			statics.noCategory.getAndIncrement();
			return;
		}

		if(!messageQueue.offer(new HinaEvent(eventByte, category))) {
			statics.queueFull.getAndIncrement();
		}
	}
	
	private String getEventCategory(byte[] propertysByte) {
		String propertys = new String(propertysByte);
		String[] aProperty = propertys.split(",");
		
		String category = null;
		for (String p : aProperty) {
			String[] keyValues = p.split(":");
			if (keyValues.length == 2 && keyValues[0].trim().equals("category")) {
				category = keyValues[1].trim();
			}
		}
		return category;
	}
	
	private int searchIndex(byte[] bytes, byte key) {

		int length = bytes.length;

		for (int i = length - 1; i >= 0; i--) {
			if (bytes[i] == key) {
				return i;
			}
		}
		return -1;
	}
	
	@Override
	public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.debug("channel %s is closed", e.getChannel().getLocalAddress().toString());
		
		e.getChannel().close();
		// TODO Auto-generated method stub
		super.channelClosed(ctx, e);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.warn("channel %s is closed: %s", e.getChannel().getLocalAddress().toString(), e.getCause().toString());
		
		e.getChannel().close();
		// TODO Auto-generated method stub
		super.exceptionCaught(ctx, e);
	}
}
