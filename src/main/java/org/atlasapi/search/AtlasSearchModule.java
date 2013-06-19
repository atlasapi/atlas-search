package org.atlasapi.search;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.channel.MongoChannelGroupStore;
import org.atlasapi.media.channel.MongoChannelStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.mongo.MongoContentLister;
import org.atlasapi.persistence.content.mongo.MongoContentResolver;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.search.searcher.BroadcastBooster;
import org.atlasapi.search.searcher.ChannelGroupBroadcastChannelBooster;
import org.atlasapi.search.searcher.LuceneSearcherProbe;
import org.atlasapi.search.searcher.ReloadingContentBootstrapper;
import org.atlasapi.search.view.JsonSearchResultsView;
import org.atlasapi.search.www.WebAwareModule;
import org.springframework.context.annotation.Bean;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.metabroadcast.common.health.HealthProbe;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.common.webapp.health.HealthController;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.search.loader.ContentBootstrapper;
import org.atlasapi.search.searcher.LuceneContentIndex;
import org.joda.time.Duration;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

public class AtlasSearchModule extends WebAwareModule {

	private final String mongoHost = Configurer.get("mongo.host").get();
	private final String mongoDbName = Configurer.get("mongo.dbName").get();
	private final Integer mongoDbPort = Configurer.get("mongo.port").toInt();
	private final String cassandraEnv = Configurer.get("cassandra.env").get();
    private final String cassandraSeeds = Configurer.get("cassandra.seeds").get();
    private final String cassandraPort = Configurer.get("cassandra.port").get();
    private final String cassandraConnectionTimeout = Configurer.get("cassandra.connectionTimeout").get();
    private final String cassandraRequestTimeout = Configurer.get("cassandra.requestTimeout").get();
    private final String luceneDir = Configurer.get("lucene.contentDir").get();
    private final String luceneIndexAtStartup = Configurer.get("lucene.indexAtStartup", "").get();
	private final String enablePeople = Configurer.get("people.enabled").get();
	private final String enableMusic = Configurer.get("music.enabled").get();
	private final String enableCassandra = Configurer.get("cassandra.enabled").get();
	private final String priorityChannelGroup = Configurer.get("priorityChannelGroup").get();

	@Override
	public void configure() {
	    BroadcastBooster booster = new ChannelGroupBroadcastChannelBooster(mongoChannelGroupStore(), channelResolver(), priorityChannelGroup);
        LuceneContentIndex index = new LuceneContentIndex(
                new File(luceneDir), 
                new MongoContentResolver(mongo(), new MongoLookupEntryStore(mongo())), 
                booster
        );
        
        Builder<HealthProbe> probes = ImmutableList.builder();
        
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(3);
        ReloadingContentBootstrapper mongoBootstrapper = new ReloadingContentBootstrapper(index, mongoBootstrapper(), scheduler, Boolean.valueOf(luceneIndexAtStartup), 180, TimeUnit.MINUTES);
        probes.add(new LuceneSearcherProbe("mongo-lucene", mongoBootstrapper, Duration.standardHours(24)));
        
        ReloadingContentBootstrapper cassandraBootstrapper = null;
        if(Boolean.valueOf(enableCassandra)) {
            cassandraBootstrapper = new ReloadingContentBootstrapper(index, cassandraBootstrapper(),scheduler,  Boolean.valueOf(luceneIndexAtStartup), 7, TimeUnit.DAYS);
            probes.add(new LuceneSearcherProbe("cassandra-lucene", cassandraBootstrapper, Duration.standardDays(9)));
        }
        
        ReloadingContentBootstrapper musicBootStrapper = null;
        if(Boolean.valueOf(enableMusic)) {
            musicBootStrapper = new ReloadingContentBootstrapper(index, musicBootstrapper(), scheduler, true, 120, TimeUnit.MINUTES);
            probes.add(new LuceneSearcherProbe("mongo-music", musicBootStrapper, Duration.standardHours(24)));
        }
        
		bind("/system/health", new HealthController(probes.build()));
		bind("/titles", new SearchServlet(new JsonSearchResultsView(), index));
		
		mongoBootstrapper.start();
		
		if(cassandraBootstrapper != null) {
		    cassandraBootstrapper.start();
		}
		
		if(musicBootStrapper != null) {
		    musicBootStrapper.start();
		}
	}
	
    @Bean
    ContentBootstrapper mongoBootstrapper() {
        
        ContentListingCriteria criteria = defaultCriteria()
                .forPublishers(ImmutableSet.<Publisher>builder()
                        .add(Publisher.PA) 
                        .addAll(Publisher.all())
                        .build()
                        .asList()
                        )
                .forContent(ImmutableSet.of(ContentCategory.CONTAINER, ContentCategory.TOP_LEVEL_ITEM))
                .build();
        
        ContentBootstrapper bootstrapper = new ContentBootstrapper(criteria);
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

    public @Bean ChannelResolver channelResolver() {
        return new MongoChannelStore(mongo(), mongoChannelGroupStore(), mongoChannelGroupStore());
    }
    
    public @Bean MongoChannelGroupStore mongoChannelGroupStore() {
        return new MongoChannelGroupStore(mongo());
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
		    System.out.println(cassandraEnv);
			CassandraContentStore cassandraContentStore = new CassandraContentStore(
			    cassandraEnv,
			    Lists.newArrayList(Splitter.on(',').split(cassandraSeeds)), 
                Integer.parseInt(cassandraPort), 
                Runtime.getRuntime().availableProcessors() * 10, 
                Integer.parseInt(cassandraConnectionTimeout), 
                Integer.parseInt(cassandraRequestTimeout));
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
                    return new ServerAddress(input, mongoDbPort);
                } catch (UnknownHostException e) {
                    return null;
                }
            }
        }), Predicates.notNull()));
    }
}
