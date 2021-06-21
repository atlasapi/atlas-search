package org.atlasapi.search.www;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.search.Utils;
import org.atlasapi.search.searcher.LuceneContentIndex;
import org.springframework.stereotype.Controller;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

@Controller
public class DocumentController extends HttpServlet {
    private static final Log log = LogFactory.getLog(DocumentController.class);

    private static final String URI_PARAMETER = "uri";
    private final LuceneContentIndex index;
    
    public DocumentController(LuceneContentIndex index) {
        this.index = checkNotNull(index);
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        log.info(Utils.fullRequestURL(request));
        String parameter = request.getParameter(URI_PARAMETER);
        response.getWriter().append(index.document(parameter).get());
    }
}
