package org.atlasapi.search;

import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentSearcher;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.view.JsonSearchResultsView;
import org.atlasapi.search.www.HealthController;
import org.atlasapi.search.www.WebAwareModule;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.mongodb.Mongo;

public class AtlasSearchModule extends WebAwareModule {

	
	@Override
	public void configure() {

		LuceneContentSearcher lucene = new LuceneContentSearcher();
		
		bind("/health", new HealthController(ImmutableList.<HealthProbe>of(new LuceneSearcherProbe(lucene))));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), lucene));
		
		new MongoDbBackedContentBootstrapper(lucene, new MongoDbBackedContentStore(mongo())).start();
	}

	private DatabasedMongo mongo() {
		try {
			return new DatabasedMongo(new Mongo(), Configurer.get("mongo.dbName").get());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
