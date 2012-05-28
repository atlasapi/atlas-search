package org.atlasapi.search.searcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.search.DebuggableContentSearcher;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.atlasapi.search.searcher.LuceneContentSearcher.IndexStats;
import org.joda.time.DateTime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class ReloadingContentSearcher extends AbstractService implements DebuggableContentSearcher {

    private static final long DELAY = 180;
    private final AtomicReference<LuceneContentSearcher> primary;
    
    private final ScheduledExecutorService executor;
    private final MongoDbBackedContentBootstrapper contentBootstrapper;
    private final Log log = LogFactory.getLog(ReloadingContentSearcher.class);
    private final KnownTypeContentResolver contentResolver;
    private final Clock clock;
    private DateTime lastIndexBuild;
    
    public ReloadingContentSearcher(MongoDbBackedContentBootstrapper contentBootstrapper, KnownTypeContentResolver contentResolver) {
        this(contentBootstrapper, contentResolver, Executors.newSingleThreadScheduledExecutor());
    }
    
    public ReloadingContentSearcher(MongoDbBackedContentBootstrapper contentBootstrapper, KnownTypeContentResolver contentResolver, ScheduledExecutorService executor) {
        this.contentBootstrapper = contentBootstrapper;
        this.contentResolver = contentResolver;
        this.primary = new AtomicReference<LuceneContentSearcher>(new LuceneContentSearcher(contentResolver));
        this.executor = executor;
        this.clock = new SystemClock();
    }
    
    @Override
    protected void doStart() {
        new Thread() {
            public void run() {
                kickOffBootstrap();
            };
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
            this.contentBootstrapper.loadAllIntoListener(primary.get());
            lastIndexBuild = clock.now();
        } catch (Exception e) {
            log.error("Exception bootstrapping", e);
        }
        this.executor.scheduleWithFixedDelay(new LoadAndSwapContentSearcher(), DELAY, DELAY, TimeUnit.MINUTES);
    }

    @Override
    public SearchResults search(SearchQuery query) {
        return primary.get().search(query);
    }
    
    class LoadAndSwapContentSearcher implements Runnable {
        @Override
        public void run() {
            try {
                log.info("Swapping content searchers");
                LuceneContentSearcher newSearcher = new LuceneContentSearcher(contentResolver);
                contentBootstrapper.loadAllIntoListener(newSearcher);
                primary.set(newSearcher);
                lastIndexBuild = clock.now();
                log.info("Finished swapping content searchers");
            } catch (Exception e) {
                log.error("Exception swapping content searchers", e);
            }
        }
    }
    
    public DateTime lastIndexBuild() {
        return lastIndexBuild;
    }
    
    public IndexStats stats() {
        return primary.get().stats();
    }

    @Override
    public String debug(SearchQuery query) {
        return primary.get().debug(query);
    }
}
