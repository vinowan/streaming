package com.tencent.qqlive.streaming.spout;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HinaSourceServer {
	private static final Logger logger = LoggerFactory.getLogger(HinaSourceServer.class);
	
	private static final int maxFrameLength = 8096;
	private String address = null;
	private int port = 0;
	
	private ServerBootstrap bootstrap = null;
	private Channel bossChannel = null;
	
	private BlockingQueue<HinaEvent> messageQueue = null;
	
	private SpoutStatics statics = null;
	
	public HinaSourceServer(BlockingQueue<HinaEvent> messageQueue, SpoutStatics statics) {
		this.messageQueue = messageQueue;
		this.statics = statics;
	}
	
	public String getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}
	
	public void startServer() throws Exception {
		bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));
		
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline(
						new LengthFieldBasedFrameDecoder(maxFrameLength, 0, 4, 0, 4),
						new HinaChannelHandler(messageQueue, statics));
				return pipeline;
			}
		});
		
		bossChannel = bootstrap.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
		
		String host = bossChannel.getLocalAddress().toString();
		int idx = host.indexOf(":");
		this.address = host.substring(1, idx);
		this.port = Integer.valueOf(host.substring(idx + 1));
	}
	
	public void stopServer() {
		if (bossChannel != null) {
			bossChannel.close();
		}
		
		if (bootstrap != null) {
			bootstrap.releaseExternalResources();
		}
	}
}
