package org.atlasapi.search;

import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentSearcher;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.view.JsonSearchResultsView;
import org.atlasapi.search.www.HealthController;
import org.atlasapi.search.www.WebAwareModule;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.mongodb.Mongo;

public class AtlasSearchModule extends WebAwareModule {

	private final String mongoHost = Configurer.get("mongo.host").get();
	private final String dbName = Configurer.get("mongo.dbName").get();

	@Override
	public void configure() {

		LuceneContentSearcher lucene = new LuceneContentSearcher();
		
		bind("/health", new HealthController(ImmutableList.<HealthProbe>of(new LuceneSearcherProbe(lucene))));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), lucene));
		
		new MongoDbBackedContentBootstrapper(lucene, new MongoDbBackedContentStore(mongo())).start();
	}

	public @Bean DatabasedMongo mongo() {
		try {
			return new DatabasedMongo(new Mongo(mongoHost), dbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
