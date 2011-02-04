package org.atlasapi.search;

import java.io.File;
import java.security.ProtectionDomain;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppContext;

public class AtlasSearchMain {

	private static final String LOCAL_WAR_DIR = "./src/main/webapp";

	public static void main(String[] args) throws Exception {
		
		Server server = createServer();
		
		WebAppContext ctx = new WebAppContext(warBase(), "/");
		
		server.setHandler(ctx);
		server.start();
		server.join();
	}
	
	private static String warBase() {
		if (new File(LOCAL_WAR_DIR).exists()) {
			return LOCAL_WAR_DIR;
		}
		ProtectionDomain domain = AtlasSearchMain.class.getProtectionDomain();
		System.out.println(domain.getCodeSource().getLocation().toString());
        return domain.getCodeSource().getLocation().toString();
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
		
		connector.setRequestBufferSize(1024);
		connector.setResponseHeaderSize(1024);
		
		server.setConnectors(new Connector[] { connector });
		return server;
	}

}
