package org.atlasapi.search;

import java.io.File;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.channel.CachingChannelStore;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.MongoProgressStore;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.atlasapi.search.searcher.BroadcastBooster;
import org.atlasapi.search.searcher.ChannelGroupBroadcastChannelBooster;
import org.atlasapi.search.searcher.LuceneContentIndex;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.searcher.ReloadingContentBootstrapper;
import org.atlasapi.search.view.JsonSearchResultsView;
import org.atlasapi.search.www.BackupController;
import org.atlasapi.search.www.ContentIndexController;
import org.atlasapi.search.www.DocumentController;
import org.atlasapi.search.www.WebAwareModule;

import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoSecondaryReadPreferenceBuilder;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.webapp.health.HealthController;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.joda.time.Duration;
import org.springframework.context.annotation.Bean;

import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

public class AtlasSearchModule extends WebAwareModule {

	private final String mongoHost = Configurer.get("mongo.host").get();
	private final String mongoDbName = Configurer.get("mongo.dbName").get();
	private final Integer mongoDbPort = Configurer.get("mongo.port").toInt();
    private final String cassandraCluster = Configurer.get("cassandra.cluster").get();
    private final String cassandraKeyspace = Configurer.get("cassandra.keyspace").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final int cassandraPort = Configurer.get("cassandra.port").toInt();
    private final int cassandraMaxConnectionsPerHost = Configurer.get("cassandra.maxConnectionsPerHost").toInt();
    private final int cassandraMaxBlockedThreadsPerHost = Configurer.get("cassandra.maxBlockedThreadsPerHost").toInt();
    private final int cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout").toInt();
    private final int cassandraRequestTimeout = Configurer.get("cassandra.requestTimeout").toInt();
    private final String luceneDir = Configurer.get("lucene.contentDir").get();
    private final String luceneIndexAtStartup = Configurer.get("lucene.indexAtStartup", "").get();
	private final String enablePeople = Configurer.get("people.enabled").get();
	private final String enableMusic = Configurer.get("music.enabled").get();
	private final String enableCassandra = Configurer.get("cassandra.enabled").get();
	private final String priorityChannelGroup = Configurer.get("priorityChannelGroup").get();
	private final String mongoTag = Strings.emptyToNull(Configurer.get("mongo.db.tag").get());
    private final String mongoFallbackTag = Strings.emptyToNull(Configurer.get("mongo.db.tag.fallback").get());
    private final String backupDirectory = Strings.emptyToNull(Configurer.get("backup.directory").get());

    private final MongoSecondaryReadPreferenceBuilder secondaryReadPreferenceBuilder = new MongoSecondaryReadPreferenceBuilder();

    @Override
	public void configure() {
	    MongoChannelGroupStore channelGroupStore = new MongoChannelGroupStore(mongo());
	    MongoLookupEntryStore lookupEntryStore = contentLookupEntryStore();
	    MongoContentResolver contentResolver = contentResolver();
	    BroadcastBooster booster = new ChannelGroupBroadcastChannelBooster(mongoChannelGroupStore(), channelResolver(), priorityChannelGroup);
	    CachingChannelStore channelStore = new CachingChannelStore(new MongoChannelStore(mongo(), channelGroupStore, channelGroupStore));
	    SimpleScheduler simplescheduler = new SimpleScheduler();

	    channelStore.start();
        LuceneContentIndex index = new LuceneContentIndex(
                new File(luceneDir), 
                contentResolver, 
                booster,
                channelStore, 
                backupDirectory
        );

        IndexBackupScheduledTask indexBackupTask = new IndexBackupScheduledTask(index);
        simplescheduler.schedule(indexBackupTask, RepetitionRules.every(Duration.standardHours(24)));

        Builder<HealthProbe> probes = ImmutableList.builder();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        ReloadingContentBootstrapper mongoBootstrapper = new ReloadingContentBootstrapper(index, mongoBootstrapper(), scheduler, Boolean.valueOf(luceneIndexAtStartup), 180, TimeUnit.MINUTES);
        probes.add(new LuceneSearcherProbe("mongo-lucene", mongoBootstrapper));

        ReloadingContentBootstrapper cassandraBootstrapper = null;
        if(Boolean.valueOf(enableCassandra)) {
            cassandraBootstrapper = new ReloadingContentBootstrapper(index, cassandraBootstrapper(),scheduler,  Boolean.valueOf(luceneIndexAtStartup), 7, TimeUnit.DAYS);
            probes.add(new LuceneSearcherProbe("cassandra-lucene", cassandraBootstrapper));
        }

        ReloadingContentBootstrapper musicBootStrapper = null;
        if(Boolean.valueOf(enableMusic)) {
            musicBootStrapper = new ReloadingContentBootstrapper(index, musicBootstrapper(), scheduler, true, 120, TimeUnit.MINUTES);
            probes.add(new LuceneSearcherProbe("mongo-music", musicBootStrapper));
        }

        // TODO: these need to be created using @Bean methods for any autowiring to work
		bind("/system/health", new HealthController(probes.build()));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), index));
		bind("/debug/document", new DocumentController(index));
		bind("/index", new ContentIndexController(new LookupResolvingContentResolver(contentResolver, lookupEntryStore), index));
		bind("/system/backup", new BackupController(index));

		mongoBootstrapper.startAsync();

		if(cassandraBootstrapper != null) {
		    cassandraBootstrapper.startAsync();
		}

		if(musicBootStrapper != null) {
		    musicBootStrapper.startAsync();
		}
	}

	private ReadPreference readPreference() {
    	ImmutableList.Builder<String> tags = ImmutableList.builder();
        if (mongoTag != null) {
            tags.add(mongoTag);
        }
        
        if (mongoFallbackTag != null) {
            tags.add(mongoFallbackTag);
        }
        
        return secondaryReadPreferenceBuilder.fromProperties(tags.build());
	}
	
	@Bean 
	MongoLookupEntryStore contentLookupEntryStore() {
	    return new MongoLookupEntryStore(mongo().collection("lookup"), new DummyPersistenceAuditLog(), readPreference());
	}
    @Bean
    ContentBootstrapper mongoBootstrapper() {
        ContentListingCriteria.Builder criteriaBuilder = defaultCriteria()
                .forPublishers(ImmutableSet.<Publisher>builder()
                        .add(Publisher.PA) 
                        .addAll(Publisher.all())
                        .build()
                        .asList()
                )
                .forContent(ImmutableSet.of(
                        ContentCategory.CONTAINER, ContentCategory.TOP_LEVEL_ITEM
                ));

        ContentBootstrapper.BuildStep bootstrapperBuilder = ContentBootstrapper.builder()
                .withTaskName("owl-search-bootstrap-mongo")
                .withProgressStore(progressStore())
                .withContentLister(new MongoContentLister(mongo(), contentResolver()))
                .withCriteriaBuilder(criteriaBuilder);

        if (Boolean.valueOf(enablePeople)) {
            LookupEntryStore entryStore = new MongoLookupEntryStore(
                    mongo().collection("peopleLookup"),
                    new DummyPersistenceAuditLog(),
                    readPreference()
            );
            MongoPersonStore personStore = new MongoPersonStore(
                    mongo(),
                    TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore),
                    entryStore,
                    new DummyPersistenceAuditLog()
            );
            bootstrapperBuilder.withPeopleLister(personStore);
        }

        return bootstrapperBuilder.build();
    }
    
    @Bean
    ContentBootstrapper cassandraBootstrapper() {
        return ContentBootstrapper.builder()
                .withTaskName("owl-search-bootstrap-cassandra")
                .withProgressStore(progressStore())
                .withContentLister(cassandra())
                .build();
    }

    @Bean
    ContentBootstrapper musicBootstrapper() {
        List<Publisher> musicPublishers = ImmutableList.of(
                Publisher.BBC_MUSIC, Publisher.SPOTIFY, Publisher.YOUTUBE, Publisher.RDIO,
                Publisher.SOUNDCLOUD, Publisher.AMAZON_UK, Publisher.ITUNES
        );

        ContentListingCriteria.Builder criteriaBuilder = defaultCriteria()
                .forContent(ContentCategory.TOP_LEVEL_ITEM)
                .forPublishers(musicPublishers);

        return ContentBootstrapper.builder()
                .withTaskName("owl-search-bootstrap-music")
                .withProgressStore(progressStore())
                .withContentLister(new MongoContentLister(mongo(), contentResolver()))
                .withCriteriaBuilder(criteriaBuilder)
                .build();
    }

    public @Bean MongoContentResolver contentResolver() {
        return new MongoContentResolver(mongo(), contentLookupEntryStore());
    }

    public @Bean ChannelResolver channelResolver() {
        return new MongoChannelStore(mongo(), mongoChannelGroupStore(), mongoChannelGroupStore());
    }
    public @Bean MongoChannelGroupStore mongoChannelGroupStore() {
        return new MongoChannelGroupStore(mongo());
    }

	public @Bean DatabasedMongo mongo() {
		try {
            MongoClientOptions.Builder options = MongoClientOptions.builder();
            options.socketKeepAlive(true);
            MongoClient mongoClient = new MongoClient(mongoHosts(), options.build());
            mongoClient.setReadPreference(readPreference());
            return new DatabasedMongo(mongoClient, mongoDbName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
    
    public @Bean CassandraContentStore cassandra() {
		try {
		    AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
                .forCluster(cassandraCluster)
                .forKeyspace(cassandraKeyspace)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                    new ConnectionPoolConfigurationImpl(cassandraCluster)
                        .setPort(cassandraPort)
                        .setMaxBlockedThreadsPerHost(cassandraMaxBlockedThreadsPerHost)
                        .setMaxConnsPerHost(cassandraMaxConnectionsPerHost)
                        .setConnectTimeout(cassandraConnectionTimeout)
                        .setSeeds(cassandraSeeds)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
		    
			CassandraContentStore cassandraContentStore = new CassandraContentStore(
			    context, 
                cassandraRequestTimeout
            );
            cassandraContentStore.init();
            return cassandraContentStore;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

    @Bean
    public MongoProgressStore progressStore() {
        return new MongoProgressStore(mongo());
    }
    
    private List<ServerAddress> mongoHosts() {
        Splitter splitter = Splitter.on(",").omitEmptyStrings().trimResults();
        return ImmutableList.copyOf(Iterables.filter(Iterables.transform(splitter.split(mongoHost), new Function<String, ServerAddress>() {

            @Override
            public ServerAddress apply(String input) {
                return new ServerAddress(input, mongoDbPort);
            }
        }), Predicates.notNull()));
    }
    
    private static class DummyPersistenceAuditLog implements PersistenceAuditLog {

        @Override
        public void logWrite(Described described) {
            // DO NOTHING
        }

        @Override
        public void logNoWrite(Described described) {
            // DO NOTHING
        }

        @Override
        public void logWrite(LookupEntry lookupEntry) {
            // DO NOTHING
        }

        @Override
        public void logNoWrite(LookupEntry lookupEntry) {
            // DO NOTHING
        }
    }
}
