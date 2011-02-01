package org.atlasapi.search.www;

import javax.servlet.http.HttpServlet;

public abstract class WebAwareModule implements Module {

	private final ServletBinder binder;
	
	public WebAwareModule() {
		binder = DispatchingServlet.INSTANCE;
	}
	
	protected void bind(String path, HttpServlet servlet) {
		binder.bind(path, servlet);
	}
}
