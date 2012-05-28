package org.atlasapi.search.searcher;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.joda.time.DateTime;

import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class ReloadingContentBootstrapper extends AbstractService {

    private volatile ContentChangeListener listener;
    private final ScheduledExecutorService executor;
    private final ContentBootstrapper contentBootstrapper;
    private final long delayInMillis;
    private final Clock clock;
    private volatile DateTime lastIndexBuild;
    private final Log log = LogFactory.getLog(ReloadingContentBootstrapper.class);

    public ReloadingContentBootstrapper(LuceneContentIndex listener, ContentBootstrapper contentBootstrapper, long delay, TimeUnit unit) {
        this(listener, contentBootstrapper, Executors.newSingleThreadScheduledExecutor(), delay, unit);
    }

    protected ReloadingContentBootstrapper(LuceneContentIndex listener, ContentBootstrapper contentBootstrapper, ScheduledExecutorService executor, long delay, TimeUnit unit) {
        this.contentBootstrapper = contentBootstrapper;
        this.listener = listener;
        this.executor = executor;
        this.delayInMillis = TimeUnit.MILLISECONDS.convert(delay, unit);
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
            this.contentBootstrapper.loadAllIntoListener(listener);
            lastIndexBuild = clock.now();
        } catch (Exception e) {
            log.error("Exception bootstrapping", e);
        }
        this.executor.scheduleWithFixedDelay(new LoadContentSearcher(), delayInMillis, delayInMillis, TimeUnit.MILLISECONDS);
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
                log.error("Exception swapping content searchers", e);
            }
        }
    }
}
