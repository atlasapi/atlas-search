package org.atlasapi.search.www;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.health.ProbeResult.ProbeResultEntry;
import com.metabroadcast.common.media.MimeType;

public class HealthController extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private Map<String, HealthProbe> probes;

	public HealthController(List<HealthProbe> probes) {
		this.probes = map(probes);
	}
	
	public void addProbes(Iterable<HealthProbe> probes) {
	    this.probes = ImmutableMap.<String, HealthProbe>builder().putAll(this.probes).putAll(map(probes)).build();
	    
	}

    private Map<String, HealthProbe> map(Iterable<HealthProbe> probes) {
        return Maps.uniqueIndex(probes, new Function<HealthProbe,String>(){
            @Override
            public String apply(HealthProbe probe) {
                return probe.slug();
            }
		});
    }
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		showHealthPage(response);
	}
   
	public String showHealthPage(HttpServletResponse response) throws IOException {
		healthPageFor(probes.values(), response);
		return null;
	}

    private void healthPageFor(Iterable<HealthProbe> selectedProbes, HttpServletResponse response) throws IOException {
        List<ProbeResult> results = runProbes(selectedProbes);
		
		boolean success = resultFrom(results);
		
		response.setStatus(success ? HttpServletResponse.SC_OK : HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		response.setContentType(MimeType.TEXT_HTML.toString());
		response.setCharacterEncoding(Charsets.UTF_8.toString());
		
		PrintWriter out = response.getWriter();
		printHeader(out, success);
		
		for (ProbeResult probeResult : results) {
			printResult(out, probeResult);
		}
		
		printFooter(out);
		out.flush();
    }

	private void printResult(PrintWriter out, ProbeResult result) throws IOException {
		out.println("<table><tr class=\"tableheader\"><th colspan=\"2\">" + result.title() + "</th></tr>");
		for (ProbeResultEntry entry : result.entries()) {
			out.println("<tr class=\"" + entry.getType().toString().toLowerCase() + "\"><td>" + entry.getKey() + "</td><td>" + entry.getValue()+ "</td></tr>");
		}
		out.println("</table>");
	}

	private void printHeader(PrintWriter out, boolean success) throws IOException {
		out.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">" +
				    "<html xmlns=\"http://www.w3.org/1999/xhtml\">" +
				    "<head><title>Health</title>" +
				    "<style>" +
				    "* {font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; font-weight: 300;color: #333;}" +
				    "h1 {font-size:250%; font-weight:100; margin:10px 0; text-align:center; width:500px;}" +
				    ".overall {border: 1px solid #999;display: block;float: left;height: 50px;width: 50px;}" +
				    "table { padding: 10px 0; width:550px;}" +
				    "th {background-color: #EEE; border:solid #DFDFDF; border-width:1px 0px;}" + 
				    ".success { background-color: #9F9; }" +
				    ".failure { background-color: #F44; }" +
				    "td {padding: 1px 5px;}" +
				    "</style></head>" +
					"<body><h1><span class=\"overall " + (success ? "success" : "failure") + "\"> </span>Health</h1>");
	}
	
	private void printFooter(PrintWriter out) throws IOException {
		out.println("</body></html>");
	}

	private boolean resultFrom(List<ProbeResult> results) {
		for (ProbeResult result : results) {
			if (result.isFailure()) {
				return false;
			}
		}
		return true;
	}
	
	private List<ProbeResult> runProbes(Iterable<HealthProbe> probes) {
		List<ProbeResult> results = Lists.newArrayListWithCapacity(Iterables.size(probes));
		
		for (HealthProbe probe : probes) {
			try { 
				results.add(probe.probe());
			} catch (Exception e) {
				results.add(new ProbeResult(probe.title()).addFailure("exception", e.getMessage()));
			}
		}
		return results;
	}
}
