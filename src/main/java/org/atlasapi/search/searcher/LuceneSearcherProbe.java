package org.atlasapi.search.searcher;

import static com.metabroadcast.common.health.ProbeResult.ProbeResultType.INFO;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Throwables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.health.ProbeResult.ProbeResultEntry;
import com.metabroadcast.common.health.ProbeResult.ProbeResultType;

public class LuceneSearcherProbe implements HealthProbe {

    private static final String LAST_INDEX_BUILD_KEY = "last index rebuild finish time";
    private static final DateTimeFormatter dateFormat= ISODateTimeFormat.dateTime();
    private static final ProbeResultEntry NO_LAST_BUILD
        = new ProbeResultEntry(ProbeResultType.INFO, LAST_INDEX_BUILD_KEY, "nil");

    private final ReloadingContentBootstrapper index;
    private final String slug;

    public LuceneSearcherProbe(String slug, ReloadingContentBootstrapper index) {
        this.slug = slug;
        this.index = index;
    }

    @Override
    public ProbeResult probe() {
        ProbeResult result = new ProbeResult(title());
        try {
            DateTime lastIndexBuild = index.lastIndexBuild();
            result.addEntry(lastIndexBuildEntry(lastIndexBuild));
        } catch (Exception e) {
            result.add("Probe exception", Throwables.getStackTraceAsString(e), false);
        }
        return result;
    }

    private ProbeResultEntry lastIndexBuildEntry(DateTime lastIndexBuild) {
        if (lastIndexBuild == null) {
            return NO_LAST_BUILD;
        }
        return new ProbeResultEntry(INFO, LAST_INDEX_BUILD_KEY, dateFormat.print(lastIndexBuild));
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
