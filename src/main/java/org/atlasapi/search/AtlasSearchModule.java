package org.atlasapi.search;


import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoContentTables;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.search.loader.MongoDbBackedContentBootstrapper;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.searcher.ReloadingContentSearcher;
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
	private final String enablePeople = Configurer.get("people.enabled").get();

	@Override
	public void configure() {
	    
	    MongoContentResolver contentResolver = new MongoContentResolver(new MongoContentTables(mongo()));
	    ReloadingContentSearcher lucene = new ReloadingContentSearcher(bootstrapper(), contentResolver);

		bind("/health", new HealthController(ImmutableList.<HealthProbe>of(new LuceneSearcherProbe(lucene))));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), lucene));
		
		lucene.start();
	}
	
	@Bean MongoDbBackedContentBootstrapper bootstrapper() {
	    MongoDbBackedContentBootstrapper bootstrapper = new MongoDbBackedContentBootstrapper(new MongoContentLister(new MongoContentTables(mongo())));
	    if(Boolean.valueOf(enablePeople)) {
	        bootstrapper.withPeopleLister(new MongoPersonStore(mongo()));
	    }
	    return bootstrapper;
	}

	public @Bean DatabasedMongo mongo() {
		try {
			Mongo mongo = new Mongo(mongoHost);
			mongo.slaveOk();
            return new DatabasedMongo(mongo, dbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
