package org.atlasapi.search.www;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.atlasapi.search.searcher.LuceneContentIndex;

import com.metabroadcast.common.http.HttpStatusCode;


public class BackupController extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
    private final LuceneContentIndex index;

    public BackupController(LuceneContentIndex index) {
        this.index = checkNotNull(index);
    }
    
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        index.backup();
        response.setStatus(HttpStatusCode.OK.code());
    }
}
