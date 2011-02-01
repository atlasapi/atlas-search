package org.atlasapi.search;

import java.io.File;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;
import org.joda.time.Duration;

public class AtlasSearchMain {

	private static final String LOCAL_STATIC_DIR = "./src/main/webapp";

	public static void main(String[] args) throws Exception {
		
		Server server = createServer();
		
		String warUrl = warUrl();
		WebAppContext ctx = new WebAppContext(warUrl, "/");
		
		server.setHandler(ctx);
		server.start();
		
		server.join();
	}

	private static String warUrl() {
		return new File(LOCAL_STATIC_DIR).getAbsolutePath();
	}
	
	private static Server createServer() {
		int port = 8181;
		
		Server server = new Server();
		
		final SelectChannelConnector connector = new SelectChannelConnector();
		connector.setPort(port);
		
		connector.setAcceptQueueSize(2048);
		
		connector.setThreadPool(new QueuedThreadPool(500));
		
		// one acceptor per CPU (ish)
		connector.setAcceptors(4);
		
		Duration maxIdleTime = Duration.standardSeconds(15);

		connector.setMaxIdleTime((int) maxIdleTime.getMillis());
		connector.setLowResourcesMaxIdleTime((int) maxIdleTime.getMillis());
		
		connector.setRequestBufferSize(1024);
		connector.setResponseHeaderSize(1024);
		
		server.setConnectors(new Connector[] { connector });
		return server;
	}

}
