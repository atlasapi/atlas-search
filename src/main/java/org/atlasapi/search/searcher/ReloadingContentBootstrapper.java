package org.atlasapi.search.searcher;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import org.joda.time.DateTime;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import org.atlasapi.persistence.bootstrap.ContentBootstrapper;
import org.atlasapi.persistence.bootstrap.ContentChangeListener;

public class ReloadingContentBootstrapper extends AbstractService {

    private volatile ContentChangeListener listener;
    private final ScheduledExecutorService executor;
    private final ContentBootstrapper contentBootstrapper;
    private final boolean indexAtStartup;
    private final long delayInMillis;
    private final Clock clock;
    private volatile DateTime lastIndexBuild;
    private final Log log = LogFactory.getLog(ReloadingContentBootstrapper.class);

    public ReloadingContentBootstrapper(LuceneContentIndex listener, ContentBootstrapper contentBootstrapper, ScheduledExecutorService executor, boolean indexAtStartup, long delay, TimeUnit unit) {
        this.contentBootstrapper = contentBootstrapper;
        this.listener = listener;
        this.executor = executor;
        this.indexAtStartup = indexAtStartup;
        this.delayInMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
        this.clock = new SystemClock();
    }

    @Override
    protected void doStart() {
        kickOffBootstrap();
        notifyStarted();
    }

    @Override
    protected void doStop() {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    protected void kickOffBootstrap() {
        long initialDelay = indexAtStartup ? 0 : delayInMillis;
        this.executor.scheduleWithFixedDelay(new LoadContentSearcher(), initialDelay, delayInMillis, TimeUnit.MILLISECONDS);
    }

    public DateTime lastIndexBuild() {
        return lastIndexBuild;
    }

    class LoadContentSearcher implements Runnable {

        @Override
        public void run() {
            try {
                log.info("Loading content searcher");
                contentBootstrapper.loadAllIntoListener(listener);
                lastIndexBuild = clock.now();
                log.info("Finished loading content searcher");
            } catch (Exception e) {
                lastIndexBuild = null;
                log.error("Exception reloading content searchers", e);
            }
        }
    }
}