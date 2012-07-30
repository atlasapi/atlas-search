package org.atlasapi.search.searcher;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class LuceneSearcherProbe implements HealthProbe {
 
    private final Clock clock;
    private final ReloadingContentBootstrapper index;
    private final String slug;
    private final Duration maxStaleness;

    public LuceneSearcherProbe(String slug, ReloadingContentBootstrapper index, Duration maxStaleness) {
        this.slug = slug;
        this.index = index;
        this.maxStaleness = maxStaleness;
        this.clock = new SystemClock();
    }

    @Override
    public ProbeResult probe() {
        ProbeResult result = new ProbeResult(title());
        DateTime lastIndexBuild = index.lastIndexBuild();
        result.add("Last index rebuild finish time",
                lastIndexBuild != null ? lastIndexBuild.toString("dd/MM/yy HH:mm") : "nil",
                lastIndexBuild != null && clock.now().minus(maxStaleness).isBefore(lastIndexBuild));
        return result;
    }

    @Override
    public String title() {
        return "Lucene index: " + slug;
    }

    @Override
    public String slug() {
        return slug;
    }
}
