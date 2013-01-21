package org.atlasapi.search;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;
import static org.atlasapi.persistence.cassandra.CassandraSchema.getKeyspace;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentIndex;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.searcher.ReloadingContentBootstrapper;
import org.atlasapi.search.view.JsonSearchResultsView;
import org.atlasapi.search.www.WebAwareModule;
import org.joda.time.Duration;
import org.springframework.context.annotation.Bean;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.webapp.health.HealthController;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AtlasSearchModule extends WebAwareModule {

	private final String mongoHost = Configurer.get("mongo.host").get();
	private final String mongoDbName = Configurer.get("mongo.dbName").get();
	private final String cassandraEnv = Configurer.get("cassandra.env").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final String cassandraPort = Configurer.get("cassandra.port").get();
    private final String cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout").get();
    private final String cassandraRequestTimeout = Configurer.get("cassandra.requestTimeout").get();
    private final String luceneDir = Configurer.get("lucene.contentDir").get();
    private final String luceneIndexAtStartup = Configurer.get("lucene.indexAtStartup", "").get();
	private final String enablePeople = Configurer.get("people.enabled").get();
	private final String enableCassandra = Configurer.get("cassandra.enabled").get();

	@Override
	public void configure() {        
        LuceneContentIndex index = new LuceneContentIndex(new File(luceneDir), new MongoContentResolver(mongo()));
        
        Builder<HealthProbe> probes = ImmutableList.builder();
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        ReloadingContentBootstrapper mongoBootstrapper = new ReloadingContentBootstrapper(index, mongoBootstrapper(), scheduler, Boolean.valueOf(luceneIndexAtStartup), 180, TimeUnit.MINUTES);
        probes.add(new LuceneSearcherProbe("mongo-lucene", mongoBootstrapper, Duration.standardHours(24)));
        
        ReloadingContentBootstrapper cassandraBootstrapper = null;
        if(Boolean.valueOf(enableCassandra)) {
            cassandraBootstrapper = new ReloadingContentBootstrapper(index, cassandraBootstrapper(),scheduler,  Boolean.valueOf(luceneIndexAtStartup), 7, TimeUnit.DAYS);
            probes.add(new LuceneSearcherProbe("cassandra-lucene", cassandraBootstrapper, Duration.standardDays(9)));
        }
        
	    ReloadingContentBootstrapper musicBootStrapper = new ReloadingContentBootstrapper(index, musicBootstrapper(), scheduler, true, 120, TimeUnit.MINUTES);
	    probes.add(new LuceneSearcherProbe("mongo-music", musicBootStrapper, Duration.standardHours(24))); 
        
		bind("/system/health", new HealthController(probes.build()));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), index));
		
		mongoBootstrapper.start();
		
		if(cassandraBootstrapper != null) {
		    cassandraBootstrapper.start();
		}
		
        musicBootStrapper.start();
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

    @Bean
    ContentBootstrapper musicBootstrapper() {
        List<Publisher> musicPublishers = ImmutableList.of(Publisher.BBC_MUSIC, Publisher.SPOTIFY, 
            Publisher.YOUTUBE, Publisher.RDIO, Publisher.SOUNDCLOUD, 
            Publisher.AMAZON_UK, Publisher.ITUNES);
        ContentListingCriteria criteria = defaultCriteria()
                .forContent(ContentCategory.TOP_LEVEL_ITEM)
                .forPublishers(musicPublishers )
                .build();
        ContentBootstrapper bootstrapper = new ContentBootstrapper(criteria );
        bootstrapper.withContentListers(new MongoContentLister(mongo()));
        return bootstrapper;
    }

	public @Bean DatabasedMongo mongo() {
		try {
			Mongo mongo = new Mongo(mongoHosts());
			mongo.setReadPreference(ReadPreference.secondaryPreferred());
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
    
    private List<ServerAddress> mongoHosts() {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(splitter.split(mongoHost), new Function<String, ServerAddress>() {

            @Override
            public ServerAddress apply(String input) {
                try {
                    return new ServerAddress(input, 27017);
                } catch (UnknownHostException e) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }
}
