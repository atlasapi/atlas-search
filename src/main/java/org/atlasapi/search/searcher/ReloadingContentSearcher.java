package org.atlasapi.search.searcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.search.DebuggableContentSearcher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import org.atlasapi.search.loader.ContentBootstrapper;

public class ReloadingContentSearcher extends AbstractService implements DebuggableContentSearcher {

    private static final long DELAY = 180;
    private volatile LuceneContentSearcher primary;
    private final ScheduledExecutorService executor;
    private final ContentBootstrapper contentBootstrapper;
    private final Log log = LogFactory.getLog(ReloadingContentSearcher.class);

    public ReloadingContentSearcher(LuceneContentSearcher delegate, ContentBootstrapper contentBootstrapper) {
        this(delegate, contentBootstrapper, Executors.newSingleThreadScheduledExecutor());
    }

    public ReloadingContentSearcher(LuceneContentSearcher delegate, ContentBootstrapper contentBootstrapper, ScheduledExecutorService executor) {
        this.contentBootstrapper = contentBootstrapper;
        this.primary = delegate;
        this.executor = executor;
    }

    @Override
    protected void doStart() {
        new Thread() {

            public void run() {
                kickOffBootstrap();
            }
        ;
        }.start();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    protected void kickOffBootstrap() {
        try {
            this.contentBootstrapper.loadAllIntoListener(primary);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        this.executor.scheduleWithFixedDelay(new LoadContentSearcher(), DELAY, DELAY, TimeUnit.MINUTES);
    }

    @Override
    public SearchResults search(SearchQuery query) {
        return primary.search(query);
    }

    class LoadContentSearcher implements Runnable {

        @Override
        public void run() {
            try {
                log.info("Loading content searcher");
                contentBootstrapper.loadAllIntoListener(primary);
                log.info("Finished loading content searcher");
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    @Override
    public String debug(SearchQuery query) {
        return primary.debug(query);
    }
}
