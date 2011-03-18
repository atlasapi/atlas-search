package org.atlasapi.search.searcher;

import org.atlasapi.search.searcher.LuceneContentSearcher.IndexStats;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.health.ProbeResult;
import com.metabroadcast.common.units.ByteCount;

public class LuceneSearcherProbe implements HealthProbe {

	private static final ByteCount MAX_INDEX_SIZE = ByteCount.gibibytes(1);
	
	private final LuceneContentSearcher index;

	public LuceneSearcherProbe(LuceneContentSearcher index) {
		this.index = index;
	}
	
	@Override
	public ProbeResult probe() {
		ProbeResult result = new ProbeResult(title());
		IndexStats stats = index.stats();
		result.addInfo("brands index size", stats.getBrandsIndexSize().prettyPrint());
		result.add("total index size", stats.getTotalIndexSize().prettyPrint(), stats.getTotalIndexSize().isLessThan(MAX_INDEX_SIZE));
		return result;
	}

	@Override
	public String title() {
		return "Lucene index";
	}

    @Override
    public String slug() {
        return "lucene";
    }
}
