package org.atlasapi.search.www;

import com.metabroadcast.common.http.HttpStatusCode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.search.Utils;
import org.atlasapi.search.searcher.LuceneContentIndex;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;


public class BackupController extends HttpServlet {
    private static final Log log = LogFactory.getLog(BackupController.class);

    private static final long serialVersionUID = 1L;
    
    private final LuceneContentIndex index;

    public BackupController(LuceneContentIndex index) {
        this.index = checkNotNull(index);
    }
    
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        log.info(Utils.fullRequestURL(request));
        index.backup();
        response.setStatus(HttpStatusCode.OK.code());
    }
}
