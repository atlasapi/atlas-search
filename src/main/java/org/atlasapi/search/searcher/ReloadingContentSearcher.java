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
import org.joda.time.DateTime;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class ReloadingContentSearcher extends AbstractService implements DebuggableContentSearcher {

    private static final long DELAY = 180;
    private volatile LuceneContentSearcher primary;
    private final ScheduledExecutorService executor;
    private final ContentBootstrapper contentBootstrapper;
    private final Log log = LogFactory.getLog(ReloadingContentSearcher.class);

    private final Clock clock;
    private DateTime lastIndexBuild;
    
    public ReloadingContentSearcher(LuceneContentSearcher delegate, ContentBootstrapper contentBootstrapper) {
        this(delegate, contentBootstrapper, Executors.newSingleThreadScheduledExecutor());
    }
    
    public ReloadingContentSearcher(LuceneContentSearcher delegate, ContentBootstrapper contentBootstrapper, ScheduledExecutorService executor) {
        this.contentBootstrapper = contentBootstrapper;
        this.primary = delegate;
        this.executor = executor;
        this.clock = new SystemClock();
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
            lastIndexBuild = clock.now();
        } catch (Exception e) {
            log.error("Exception bootstrapping", e);
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
                lastIndexBuild = clock.now();
                log.info("Finished loading content searcher");
            } catch (Exception e) {
                log.error("Exception swapping content searchers", e);
            }
        }
    }
    
    public DateTime lastIndexBuild() {
        return lastIndexBuild;
    }
    

    @Override
    public String debug(SearchQuery query) {
        return primary.debug(query);
    }
}
