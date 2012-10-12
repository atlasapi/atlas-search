package org.atlasapi.search;


import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.searcher.ReloadingContentBootstrapper;
import org.atlasapi.search.view.JsonSearchResultsView;
import org.atlasapi.search.www.WebAwareModule;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.webapp.health.HealthController;
import com.mongodb.Mongo;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentIndex;
import org.joda.time.Duration;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

public class AtlasSearchModule extends WebAwareModule {

	private final String mongoHost = Configurer.get("mongo.host").get();
	private final String mongoDbName = Configurer.get("mongo.dbName").get();
    private final String cassandraEnv = Configurer.get("cassandra.env").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final String cassandraPort = Configurer.get("cassandra.port").get();
    private final String cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout").get();
    private final String cassandraRequestTimeout = Configurer.get("cassandra.requestTimeout").get();
    private final String luceneDir = Configurer.get("lucene.contentDir").get();
	private final String enablePeople = Configurer.get("people.enabled").get();

	@Override
	public void configure() {        
        LuceneContentIndex index = new LuceneContentIndex(new File(luceneDir), new MongoContentResolver(mongo()));
        
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ReloadingContentBootstrapper mongoBootstrapper = new ReloadingContentBootstrapper(index, mongoBootstrapper(), scheduler, 180, TimeUnit.MINUTES);
	    ReloadingContentBootstrapper cassandraBootstrapper = new ReloadingContentBootstrapper(index, cassandraBootstrapper(), scheduler, 7, TimeUnit.DAYS);
        
		bind("/system/health", new HealthController(ImmutableList.<HealthProbe>of(
                new LuceneSearcherProbe("mongo-lucene", mongoBootstrapper, Duration.standardHours(24)), 
                new LuceneSearcherProbe("cassandra-lucene", cassandraBootstrapper, Duration.standardDays(9)))));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), index));
		
		mongoBootstrapper.start();
        cassandraBootstrapper.start();
	}
	
    @Bean
    ContentBootstrapper mongoBootstrapper() {
        ContentBootstrapper bootstrapper = new ContentBootstrapper();
        bootstrapper.withContentListers(new MongoContentLister(mongo()));
        if (Boolean.valueOf(enablePeople)) {
            bootstrapper.withPeopleListers(new MongoPersonStore(mongo()));
        }
        return bootstrapper;
    }
    
    @Bean
    ContentBootstrapper cassandraBootstrapper() {
        ContentBootstrapper bootstrapper = new ContentBootstrapper();
        bootstrapper.withContentListers(cassandra());
        return bootstrapper;
    }

	public @Bean DatabasedMongo mongo() {
		try {
			Mongo mongo = new Mongo(mongoHost);
			mongo.slaveOk();
            return new DatabasedMongo(mongo, mongoDbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
    
    public @Bean CassandraContentStore cassandra() {
		try {
            AstyanaxContext<Keyspace> cassandraContext = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(getKeyspace(cassandraEnv)).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(Integer.parseInt(cassandraPort)).
                setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setConnectTimeout(Integer.parseInt(cassandraConnectionTimeout)).
                setSeeds(cassandraSeeds)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
			CassandraContentStore cassandraContentStore = new CassandraContentStore(cassandraContext, Integer.parseInt(cassandraRequestTimeout));
            cassandraContext.start();
            cassandraContentStore.init();
            return cassandraContentStore;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
