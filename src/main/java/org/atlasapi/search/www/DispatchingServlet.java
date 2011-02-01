package org.atlasapi.search.www;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import com.google.common.collect.Lists;

public final class DispatchingServlet implements Servlet, ServletBinder {

	static ServletBinder INSTANCE;
	
	private List<BoundServlet> servlets = Lists.newArrayList();
	private ServletConfig config;
	
	public DispatchingServlet bind(String path, HttpServlet servlet) {
		servlets.add(new BoundServlet(servlet, path));
		return this;
	}
	
	@Override
	public void destroy() {
		for (BoundServlet servlet : servlets) {
			servlet.destroy();
		}
	}

	@Override
	public void service(ServletRequest request, ServletResponse response) throws ServletException, IOException {
		for (BoundServlet servlet : servlets) {
			if (servlet.matches(((HttpServletRequest) request).getRequestURI())) {
				servlet.service(request, response);
			}
		}
	}

	@Override
	public ServletConfig getServletConfig() {
		return config;
	}

	@Override
	public String getServletInfo() {
		return getClass().getName();
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		INSTANCE = this;
		try {
			Module module = (Module) Class.forName(config.getInitParameter("module")).newInstance();
			module.configure();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		this.config = config;
		for (BoundServlet servlet : servlets) {
			servlet.init(config);
		}
	}
	
	private static class BoundServlet {
		
		private final HttpServlet servlet;
		private Pattern pathPatten;

		public BoundServlet(HttpServlet servlet, String path) {
			this.servlet = servlet;
			this.pathPatten = Pattern.compile(path);
		}
		
		public void init(ServletConfig config) throws ServletException {
			servlet.init(config);
		}

		public void destroy() {
			servlet.destroy();
		}

		boolean matches(String path) {
			return pathPatten.matcher(path).matches();
		}

		void service(ServletRequest request, ServletResponse response) throws ServletException, IOException {
			servlet.service(request, response);
		}
	}
}
