/* Copyright 2010 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.search.searcher;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermsFilter;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.function.CustomScoreQuery;
import org.apache.lucene.search.function.IntFieldSource;
import org.apache.lucene.search.function.ValueSourceQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Version;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.lookup.entry.LookupRef;
import org.atlasapi.search.DebuggableContentSearcher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;
import java.io.File;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.store.FSDirectory;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Specialization;

public class LuceneContentSearcher implements ContentChangeListener, DebuggableContentSearcher {

    private static final int MAX_RESULTS = 1000;
    private static final Log log = LogFactory.getLog(LuceneContentSearcher.class);
    static final String FIELD_TITLE_FLATTENED = "title-flattened";
    static final String FIELD_CONTENT_TITLE = "title";
    static final String FIELD_CONTENT_SPECIALIZATION = "specialization";
    static final String FIELD_CONTENT_PUBLISHER = "publisher";
    private static final String FIELD_CONTENT_URI = "contentUri";
    private static final String FIELD_AVAILABLE = "available";
    private static final String FIELD_BROADCAST_HOUR_TS = "broadcast";
    private static final String FIELD_PRIORITY_CHANNEL = "priorityChannel";
    private static final String FIELD_FIRST_BROADCAST = "firstBroadcast";
    
    private static final Set<String> PRIORITY_CHANNELS = ImmutableSet.<String>builder()
            .add("http://www.bbc.co.uk/services/bbcone/london")
            .add("http://www.bbc.co.uk/services/bbcone/ni")
            .add("http://www.bbc.co.uk/services/bbcone/cambridge")
            .add("http://www.bbc.co.uk/services/bbcone/channel_islands")
            .add("http://www.bbc.co.uk/services/bbcone/east")
            .add("http://www.bbc.co.uk/services/bbcone/east_midlands")
            .add("http://www.bbc.co.uk/services/bbcone/hd")
            .add("http://www.bbc.co.uk/services/bbcone/north_east")
            .add("http://www.bbc.co.uk/services/bbcone/north_west")
            .add("http://www.bbc.co.uk/services/bbcone/oxford")
            .add("http://www.bbc.co.uk/services/bbcone/scotland")
            .add("http://www.bbc.co.uk/services/bbcone/south")
            .add("http://www.bbc.co.uk/services/bbcone/south_east")
            .add("http://www.bbc.co.uk/services/bbcone/wales")
            .add("http://www.bbc.co.uk/services/bbcone/south_west")
            .add("http://www.bbc.co.uk/services/bbcone/west")
            .add("http://www.bbc.co.uk/services/bbcone/west_midlands")
            .add("http://www.bbc.co.uk/services/bbcone/east_yorkshire")
            .add("http://www.bbc.co.uk/services/bbcone/yorkshire")
            .add("http://www.bbc.co.uk/services/bbctwo/england")
            .add("http://www.bbc.co.uk/services/bbctwo/ni")
            .add("http://www.bbc.co.uk/services/bbctwo/ni_analogue")
            .add("http://www.bbc.co.uk/services/bbctwo/scotland")
            .add("http://www.bbc.co.uk/services/bbctwo/wales")
            .add("http://www.bbc.co.uk/services/bbctwo/wales_analogue")
    		.add("http://www.itv.com/channels/itv1/anglia")
    		.add("http://www.itv.com/channels/itv1/bordersouth")
    		.add("http://www.itv.com/channels/itv1/london")
    		.add("http://www.itv.com/channels/itv1/carltoncentral")
    		.add("http://www.itv.com/channels/itv1/channel")
    		.add("http://www.itv.com/channels/itv1/granada")
    		.add("http://www.itv.com/channels/itv1/meridian")
    		.add("http://www.itv.com/channels/itv1/tynetees")
    		.add("http://ref.atlasapi.org/channels/ytv")
    		.add("http://www.itv.com/channels/itv1/carltonwestcountry")
    		.add("http://www.itv.com/channels/itv1/wales")
    		.add("http://www.itv.com/channels/itv1/west")
    		.add("http://ref.atlasapi.org/channels/stvcentral")
    		.add("http://ref.atlasapi.org/channels/ulster")
    		.add("http://www.itv.com/channels/itv1/bordernorth")
    		.add("http://www.channel4.com")
    		.add("http://ref.atlasapi.org/channels/s4c")
    		.add("http://www.five.tv")
    		.add("http://www.bbc.co.uk/services/bbcthree")
    		.add("http://www.bbc.co.uk/services/bbcfour")
    		.add("http://www.itv.com/channels/itv2")
    		.add("http://www.itv.com/channels/itv3")
    		.add("http://www.itv.com/channels/itv4")
    		.add("http://www.e4.com")
    		.add("http://www.channel4.com/more4")
    		.add("http://film4.com")
    		.add("http://ref.atlasapi.org/channels/sky1")
    		.add("http://ref.atlasapi.org/channels/skyatlantic")
    		.add("http://ref.atlasapi.org/channels/dave")
    		.add("http://www.bbc.co.uk/services/bbchd")
    		.add("http://ref.atlasapi.org/channels/watch")
    		.add("http://ref.atlasapi.org/channels/gold")
    		.add("http://ref.atlasapi.org/channels/comedycentral")
    		.add("http://ref.atlasapi.org/channels/skysports1")
    		.add("http://ref.atlasapi.org/channels/skysports2")
    		.add("http://ref.atlasapi.org/channels/sky3")
    		.add("http://ref.atlasapi.org/channels/sky2")
    		.add("http://www.five.tv/channels/fiver")
    		.add("http://www.five.tv/channels/five-usa")
    		.add("http://www.bbc.co.uk/services/cbeebies")
    		.add("http://www.bbc.co.uk/services/cbbc")
    		.add("http://www.itv.com/channels/citv")
    		.add("http://ref.atlasapi.org/channels/skyliving")
    		.add("http://www.bbc.co.uk/services/radio1/england")
    		.add("http://www.bbc.co.uk/services/radio2")
    		.add("http://www.bbc.co.uk/services/radio3")
    		.add("http://www.bbc.co.uk/services/radio4/fm")
    		.add("http://www.bbc.co.uk/services/radio7")
    		.add("http://www.bbc.co.uk/services/radio4/lw")
    		.add("http://www.bbc.co.uk/services/5live")
    		.add("http://www.bbc.co.uk/services/5livesportsextra")
    		.add("http://www.bbc.co.uk/services/6music")
    		.add("http://www.bbc.co.uk/services/1xtra")
    		.add("http://www.bbc.co.uk/services/asiannetwork")
    		.add("http://www.bbc.co.uk/services/worldservice")
    		.build();
    
    private static final int HOURS_IN_A_WEEK = 168;
    
    private static final int TRUE = 1;
    private static final int FALSE = 0;
    
    private static final float TOLERANCE = 0.000001f;
    
    private static final TitleQueryBuilder titleQueryBuilder = new TitleQueryBuilder();
    private static final Timestamper clock = new SystemClock();
    private final Directory contentDir;
    private final KnownTypeContentResolver contentResolver;
    private Duration maxBroadcastAgeForInclusion = Duration.standardDays(365);

    public LuceneContentSearcher(File luceneDir, KnownTypeContentResolver contentResolver) {
        this.contentResolver = contentResolver;
        try {
            this.contentDir = FSDirectory.open(luceneDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SearchResults search(SearchQuery q) {
        return new SearchResults(search(searcherFor(contentDir), getQuery(q), getFilter(q), q.getSelection()));
    }

    @Override
    public String debug(SearchQuery q) {
        return Joiner.on("\n").join(debug(searcherFor(contentDir), getQuery(q), getFilter(q), q.getSelection()));
    }

    @Override
    public void contentChange(Iterable<? extends Described> contents) {
        IndexWriter writer = null;
        try {
            writer = writerFor(contentDir);
            writer.setWriteLockTimeout(5000);
            for (Described content : Iterables.filter(contents, FILTER_SEARCHABLE_CONTENT)) {
                Document doc = asDocument(content);
                if (doc != null) {
                    writer.updateDocument(new Term(FIELD_CONTENT_URI, content.getCanonicalUri()), doc);
                } else if (log.isInfoEnabled()) {
                    log.info("Content with title " + content.getTitle() + " and uri " + content.getCanonicalUri() + " not added due to null elements");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (writer != null) {
                closeWriter(writer);
            }
        }
    }

    private void closeWriter(IndexWriter writer) {
        try {
            writer.commit();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                // not much that can be done here
                throw new RuntimeException(e);
            }
        }
    }

    private static IndexWriter writerFor(Directory dir) throws CorruptIndexException, LockObtainFailedException, IOException {
        return new IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
    }

    private Document asDocument(Described content) {
        if (Strings.isNullOrEmpty(content.getCanonicalUri()) || Strings.isNullOrEmpty(content.getTitle()) || content.getPublisher() == null) {
            return null;
        }
        Document doc = new Document();
        doc.add(new Field(FIELD_CONTENT_TITLE, content.getTitle(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_TITLE_FLATTENED, titleQueryBuilder.flatten(content.getTitle()), Field.Store.YES, Field.Index.NOT_ANALYZED));
        doc.add(new Field(FIELD_CONTENT_URI, content.getCanonicalUri(), Field.Store.YES,  Field.Index.NOT_ANALYZED));
        if (content.getSpecialization() != null) {
            doc.add(new Field(FIELD_CONTENT_SPECIALIZATION, content.getSpecialization().toString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
        }
        if (content.getPublisher() != null) {
            doc.add(new Field(FIELD_CONTENT_PUBLISHER, content.getPublisher().toString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
        }
        if (!addBroadcastAndAvailabilityFields(content, doc)) {
            return null;
        }
        return doc;
    }

    private boolean addBroadcastAndAvailabilityFields(Described content, Document doc) {
        Timestamp now = clock.timestamp();
        int minHourTimestamp = hourOf(Timestamp.of(now.toDateTimeUTC().minus(maxBroadcastAgeForInclusion)));

        if (content instanceof Song) {
            return true;
        } else if (content instanceof Item) {
            Item item = (Item) content;
            if (item.isAvailable()) {
                doc.add(new Field(FIELD_AVAILABLE, String.valueOf(TRUE), Field.Store.NO, Field.Index.NOT_ANALYZED));
            }
            Maybe<Broadcast> closestBroadcast = closestBroadcast(item.flattenBroadcasts(), now);
            int hourOfClosestBroadcast = closestBroadcast.hasValue() ? hourOf(closestBroadcast.requireValue()) : 0;

            if (content instanceof Film) {
                // Films should pretend to be at most 30 days old (to keep cinema films in the search)
                hourOfClosestBroadcast = Math.max(hourOf(now.minus(Duration.standardDays(30))), hourOfClosestBroadcast);
            }

            if (hourOfClosestBroadcast < minHourTimestamp) {
                return false;
            }

            addBroadcastInformation(doc, hourOfClosestBroadcast, closestBroadcast);
            return true;

        } else if (content instanceof Container) {
            Container container = (Container) content;
            if (!container.getChildRefs().isEmpty()) {
                List<LookupRef> lookupRefs = LookupRef.fromChildRefs(container.getChildRefs(), container.getPublisher());

                Iterable<Item> items = Iterables.filter(contentResolver.findByLookupRefs(lookupRefs).getAllResolvedResults(), Item.class);
                if (haveAvailable(items)) {
                    doc.add(new Field(FIELD_AVAILABLE, String.valueOf(TRUE), Field.Store.NO, Field.Index.NOT_ANALYZED));
                }

                Maybe<Broadcast> closestBroadcastForItems = closestBroadcastForItems(items, now);
                if (closestBroadcastForItems.isNothing() || hourOf(closestBroadcastForItems.requireValue()) < minHourTimestamp) {
                    return false;
                }
                addBroadcastInformation(doc, hourOf(closestBroadcastForItems.requireValue()), closestBroadcastForItems);
                return true;
            }
        }
        return false;
    }
    
    private void addBroadcastInformation(Document doc, int hourOfClosestBroadcast, Maybe<Broadcast> closestBroadcast) {
    	doc.add(new NumericField(FIELD_BROADCAST_HOUR_TS, Field.Store.YES, true).setIntValue(hourOfClosestBroadcast));
    	
    	if(closestBroadcast.hasValue()) {
	        doc.add(new NumericField(FIELD_PRIORITY_CHANNEL, Field.Store.YES, true).setIntValue(isOnPriorityChannel(closestBroadcast.requireValue())));
	        doc.add(new NumericField(FIELD_FIRST_BROADCAST, Field.Store.YES, true).setIntValue(isFirstBroadcast(closestBroadcast.requireValue())));
    	}
    }
    
    private Maybe<Broadcast> closestBroadcastForItems(Iterable<Item> items, Timestamp now) {
        if (Iterables.isEmpty(items)) {
            return Maybe.nothing();
        }
        return closestBroadcast(Iterables.concat(Iterables.transform(items, Item.FLATTEN_BROADCASTS)), now);
    }
    
    private Maybe<Broadcast> closestBroadcast(Iterable<Broadcast> broadcasts, Timestamp now) {
    	Iterable<Broadcast> publishedBroadcasts = Iterables.filter(broadcasts, new Predicate<Broadcast>() {

			@Override
			public boolean apply(Broadcast input) {
				return input.isActivelyPublished();
			}
    		
    	});
    	
        if (Iterables.isEmpty(publishedBroadcasts)) {
            return Maybe.nothing();
        }

        Broadcast closest = sinceBroadcast(now).min(publishedBroadcasts);
        if (closest.getTransmissionTime() == null) {
            return Maybe.nothing();
        }
        return Maybe.just(closest);
    }

    private Ordering<Broadcast> sinceBroadcast(final Timestamp now) {
        return new Ordering<Broadcast>() {

            @Override
            public int compare(Broadcast left, Broadcast right) {
                if (left.getTransmissionTime() == null && left.getTransmissionTime() == null) {
                    return 0;
                }
                if (right.getTransmissionTime() == null) {
                    return -1;
                }
                if (left.getTransmissionTime() == null) {
                    return 1;
                }
                return Longs.compare(now.millisBetween(Timestamp.of(left.getTransmissionTime())), now.millisBetween(Timestamp.of(right.getTransmissionTime())));
            }
        };
    }

    private int isOnPriorityChannel(Broadcast broadcast) {
    	return PRIORITY_CHANNELS.contains(broadcast.getBroadcastOn()) ? TRUE : FALSE;
    }
    
    private int isFirstBroadcast(Broadcast broadcast) {
    	return Boolean.TRUE.equals(broadcast.getRepeat()) ? FALSE : TRUE;
    }
    
    private boolean haveAvailable(Iterable<Item> items) {
        for (Item item : items) {
            if (item.isAvailable()) {
                return true;
            }
        }
        return false;
    }

    private Filter getFilter(SearchQuery q) {
        return getPublisherFilter(q.getIncludedPublishers());
    }

    private Query getQuery(SearchQuery q) {
        BooleanQuery termQuery = new BooleanQuery(true);
        
        Query titleQuery = titleQueryBuilder.build(q.getTerm());
        titleQuery.setBoost(q.getTitleWeighting());
        if (!q.getIncludedSpecializations().isEmpty()) {
            Filter filter = getSpecializationFilter(q.getIncludedSpecializations());
            titleQuery = new FilteredQuery(titleQuery, filter);
        }
        termQuery.add(titleQuery, Occur.MUST);

        if (q.getCatchupWeighting() != 0.0f) {
            Query availabilityQuery = availabilityQuery(q.getCatchupWeighting());
            termQuery.add(availabilityQuery, Occur.SHOULD);
        }
        
        Query query = termQuery;
        if (q.getBroadcastWeighting() != 0.0f) {
        	query = new DistanceToBroadcastScore(termQuery).withBroadcastWeight(q.getBroadcastWeighting());
        }
        
        query = new BooleanBoostScore(query, FIELD_PRIORITY_CHANNEL).withWeighting(q.getPriorityChannelWeighting().valueOrDefault(250.0f));
        query = new BooleanBoostScore(query, FIELD_FIRST_BROADCAST).withWeighting(q.getFirstBroadcastWeighting().valueOrDefault(1.0f));
        
        return query;
    }
    
    private final static long MILLIS_IN_HOUR = Duration.standardHours(1).getMillis();

    private static int hourOf(long millis) {
        return (int) (millis / MILLIS_IN_HOUR);
    }

    private static int hourOf(Timestamp ts) {
        return hourOf(ts.millis());
    }
    
    private static int hourOf(Broadcast b) {
    	return hourOf(Timestamp.of(b.getTransmissionTime()));
    }
    
    private static class DistanceToBroadcastScore extends CustomScoreQuery {
        
        private static final long serialVersionUID = 1L;
        
        private final int currentHour;

        private float broadcastWeighting = 1;

        public DistanceToBroadcastScore(Query subQuery) {
            super(subQuery, new ValueSourceQuery(new IntFieldSource(FIELD_BROADCAST_HOUR_TS)));
            setStrict(true);
            this.currentHour = hourOf(clock.timestamp());
        }
        
        public Query withBroadcastWeight(float broadcastWeighting) {
            this.broadcastWeighting = broadcastWeighting;
            return this;
        }
        
        @Override
        public float customScore(int doc, float subQueryScore, float broadcastHour) {
        	float hoursBetweenBroadcastAndNow = Math.abs(currentHour - broadcastHour);
        	
        	// This is inverted; a higher number means we scale less. We up-weigh
        	// items broadcast or to be broadcast in the last week.
        	int scalingFactor = hoursBetweenBroadcastAndNow < HOURS_IN_A_WEEK ? 50 : 1;
        	
            float broadcastScore = (float) (1f / ((hoursBetweenBroadcastAndNow / scalingFactor) + 1));
            return subQueryScore  + (broadcastWeighting  * broadcastScore * subQueryScore);
        }
    }
    
    private static class BooleanBoostScore extends CustomScoreQuery {
    	
    	private static final long serialVersionUID = 1L;
		private float weighting;
    	
    	public BooleanBoostScore(Query subQuery, String field) {
    		super(subQuery, new ValueSourceQuery(new IntFieldSource(field)));
    		setStrict(true);
    	}
    	
    	public Query withWeighting(float weighting) {
    		this.weighting = weighting;
    		return this;
    	}
    	
    	@Override
    	public float customScore(int doc, float subQueryScore, float thisFieldValue) {
    		if(Math.abs(thisFieldValue - 1) < TOLERANCE) {
    			return weighting * subQueryScore;
    		}
    		else {
    			return subQueryScore;
    		}
    	}
    	
    }
    
    private Filter getPublisherFilter(Set<Publisher> includedPublishers) {
        TermsFilter filter = new TermsFilter();
        for (Publisher publisher : includedPublishers) {
            filter.addTerm(new Term(FIELD_CONTENT_PUBLISHER, publisher.toString()));
        }
        return filter;
    }
    
    private Filter getSpecializationFilter(Set<Specialization> includedSpecializations) {
        TermsFilter filter = new TermsFilter();
        for (Specialization specialization : includedSpecializations) {
            filter.addTerm(new Term(FIELD_CONTENT_SPECIALIZATION, specialization.toString()));
        }
        return filter;
    }

    private Query availabilityQuery(float boost) {
        TermsFilter filter = new TermsFilter();
        filter.addTerm(new Term(FIELD_AVAILABLE, String.valueOf(TRUE)));
        
        ConstantScoreQuery query = new ConstantScoreQuery(filter);
        query.setBoost(boost);
        return query;
    }

    private static Searcher searcherFor(Directory dir) {
        try {
            return new IndexSearcher(dir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static final class Result implements Comparable<Result> {

        private String uri;
        private int titleLength;
        private float score;

        private Result(ScoreDoc scoreDoc, Document doc) {
            uri = doc.getField(FIELD_CONTENT_URI).stringValue();
            titleLength = doc.getField(FIELD_TITLE_FLATTENED).stringValue().length();
            score = scoreDoc.score;
        }

        @Override
        public int compareTo(Result other) {
            int cmp = Floats.compare(other.score, score);
            if (cmp != 0) {
                return cmp;
            }
            return Ints.compare(titleLength, other.titleLength);
        }
    }
    private static final Function<Result, String> TO_URI = new Function<Result, String>() {

        @Override
        public String apply(Result input) {
            return input.uri;
        }
    };

    private List<String> search(final Searcher searcher, Query query, Filter filter, Selection selection) {
        try {
            /*
             * We re-sort the results so that when two items have the same score
             * the item with the shortest title wins.
             */
            TopDocs topDocs = getTopDocs(searcher, query, filter, selection);
            List<Result> results = Lists.newArrayList();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                results.add(new Result(scoreDoc, doc));
            }
            Collections.sort(results);
            return Lists.transform(results, TO_URI);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                searcher.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<String> debug(final Searcher searcher, Query query, Filter filter, Selection selection) {
        try {
            TopDocs topDocs = getTopDocs(searcher, query, filter, selection);

            List<String> results = Lists.newArrayList();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = searcher.doc(scoreDoc.doc);
                results.add(doc.getField(FIELD_CONTENT_URI).stringValue() + " : " + scoreDoc.score + "\n" + searcher.explain(query.weight(searcher), scoreDoc.doc));
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                searcher.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private TopDocs getTopDocs(final Searcher searcher, Query query, Filter filter, Selection selection) throws IOException {
        int startIndex = selection.getOffset();
        int endIndex = selection.limitOrDefaultValue(MAX_RESULTS);

        TopScoreDocCollector collector = TopScoreDocCollector.create(MAX_RESULTS, true);

        searcher.search(query.weight(searcher), filter, collector);

        return collector.topDocs(startIndex, endIndex);
    }
    // Stop index from growing enormous
    private final static List<Publisher> VALID_PUBLISHERS = ImmutableList.of(Publisher.BBC, Publisher.C4, Publisher.FIVE, Publisher.PA, Publisher.ITV, Publisher.SEESAW, Publisher.ITUNES, Publisher.HULU, Publisher.HBO, Publisher.PREVIEW_NETWORKS, Publisher.MUSIC_BRAINZ, Publisher.EMI_PUB);
    //
    private final static Predicate<Described> FILTER_SEARCHABLE_CONTENT = new Predicate<Described>() {

        @Override
        public boolean apply(Described input) {
            if (input instanceof Item && (!VALID_PUBLISHERS.contains(input.getPublisher()) || hasContainer((Item) input))) {
                return false;
            }
            if (input instanceof ContentGroup && !(input instanceof Person)) {
                return false;
            }

            return true;
        }
    };

    private final static boolean hasContainer(Item input) {
        return input.getContainer() != null;
    }
}
