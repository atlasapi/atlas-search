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
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.search.DebuggableContentSearcher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.joda.time.Duration;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;
import com.metabroadcast.common.units.ByteCount;

public class LuceneContentSearcher implements ContentChangeListener, DebuggableContentSearcher {

    private static final int MAX_RESULTS = 1000;
    private static final Log log = LogFactory.getLog(LuceneContentSearcher.class);
    
    static final String FIELD_TITLE_FLATTENED = "title-flattened";
    static final String FIELD_CONTENT_TITLE = "title";
    static final String FIELD_CONTENT_PUBLISHER = "publisher";
    private static final String FIELD_CONTENT_URI = "contentUri";
    private static final String FIELD_AVAILABLE = "available";
    private static final String FIELD_BROADCAST_HOUR_TS = "broadcast";
    
    private static final String TRUE = "T";
    
    private static final TitleQueryBuilder titleQueryBuilder = new TitleQueryBuilder();
    
    private final RAMDirectory contentDir = new RAMDirectory();
    private static final Timestamper clock = new SystemClock();

    private final KnownTypeContentResolver contentResolver;

    private Duration maxBroadcastAgeForInclusion = Duration.standardDays(365);
    
    public LuceneContentSearcher(KnownTypeContentResolver contentResolver) {
        this.contentResolver = contentResolver;
        try {
            formatDirectory(contentDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void closeWriter(IndexWriter writer) {
        try {
            writer.commit();
            writer.optimize();
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
    
    private static void formatDirectory(Directory dir) throws CorruptIndexException, IOException {
        IndexWriter writer = writerFor(dir);
        writer.close();
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
        doc.add(new Field(FIELD_TITLE_FLATTENED, titleQueryBuilder.flatten(content.getTitle()), Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_CONTENT_URI, content.getCanonicalUri(), Field.Store.YES,  Field.Index.NOT_ANALYZED));
        doc.add(new Field(FIELD_CONTENT_PUBLISHER, content.getPublisher().toString(), Field.Store.NO,  Field.Index.NOT_ANALYZED));
        if (!addBroadcastAndAvailabilityFields(content, doc)) {
            return null;
        }
        return doc;
    }

    private boolean addBroadcastAndAvailabilityFields(Described content, Document doc) {
        Timestamp now = clock.timestamp();
        
        int minHourTimestamp = hourOf(Timestamp.of(now.toDateTimeUTC().minus(maxBroadcastAgeForInclusion)));
        
        if (content instanceof Item) {
            Item item = (Item) content;
            if (item.isAvailable()) {
                doc.add(new Field(FIELD_AVAILABLE, TRUE, Field.Store.NO, Field.Index.NOT_ANALYZED));
            }
            int hourOfClosestBroadcast = hourOfClosestBroadcast(item.flattenBroadcasts(), now);
            
            if (hourOfClosestBroadcast == 0 && content instanceof Film) {
                // Films should pretend to be 30 days old (to keep cinema films in the search)
                hourOfClosestBroadcast = hourOf(now.minus(Duration.standardDays(30)));
            }
            
            if (hourOfClosestBroadcast < minHourTimestamp) {
                return false;
            }
            doc.add(new NumericField(FIELD_BROADCAST_HOUR_TS, Field.Store.YES, true).setIntValue(hourOfClosestBroadcast));
            return true;
            
        } else if (content instanceof Container) {
            Container container = (Container) content;
            if (!container.getChildRefs().isEmpty()) {
                List<LookupRef> lookupRefs = LookupRef.fromChildRefs(container.getChildRefs(), container.getPublisher());
                
                Iterable<Item> items = Iterables.filter(contentResolver.findByLookupRefs(lookupRefs).getAllResolvedResults(), Item.class);
                if (haveAvailable(items)) {
                    doc.add(new Field(FIELD_AVAILABLE, TRUE, Field.Store.NO, Field.Index.NOT_ANALYZED));
                }
                
                int hourOfClosestBroadcastForItems = hourOfClosestBroadcastForItems(items, now);
                if (hourOfClosestBroadcastForItems < minHourTimestamp) {
                    return false;
                }
                doc.add(new NumericField(FIELD_BROADCAST_HOUR_TS, Field.Store.YES, true).setIntValue(hourOfClosestBroadcastForItems));
                return true;
            }
        }
        return false;
    }
    
    private int hourOfClosestBroadcastForItems(Iterable<Item> items, Timestamp now) {
        if (Iterables.isEmpty(items)) {
            return 0;
        }
        return hourOfClosestBroadcast(Iterables.concat(Iterables.transform(items, Item.FLATTEN_BROADCASTS)), now);
    }
    
    private int hourOfClosestBroadcast(Iterable<Broadcast> broadcasts, Timestamp now) {
        if (Iterables.isEmpty(broadcasts)) {
            return 0;
        }
        
        Broadcast closest = sinceBroadcast(now).min(broadcasts);
        if (closest.getTransmissionTime() == null) {
            return 0;
        }
        return hourOf(Timestamp.of(closest.getTransmissionTime()));
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

    private boolean haveAvailable(Iterable<Item> items) {
        for (Item item : items) {
            if (item.isAvailable()) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public SearchResults search(SearchQuery q) {
        return new SearchResults(search(searcherFor(contentDir), getQuery(q), getFilter(q), q.getSelection()));
    }
    
    @Override
    public String debug(SearchQuery q) {
        return Joiner.on("\n").join(debug(searcherFor(contentDir), getQuery(q), getFilter(q), q.getSelection()));
    }

    private TermsFilter getFilter(SearchQuery q) {
        TermsFilter filter = new TermsFilter();
        addPublisherfilter(filter, q.getIncludedPublishers());
        return filter;
    }


    private Query getQuery(SearchQuery q) {
        BooleanQuery query = new BooleanQuery(true);
        
        Query titleQuery = titleQueryBuilder.build(q.getTerm());
        
        titleQuery.setBoost(q.getTitleWeighting());

        query.add(titleQuery, Occur.MUST);

        if (q.getCatchupWeighting() != 0.0f) {
            Query availabilityQuery = availabilityQuery(q.getCatchupWeighting());
            query.add(availabilityQuery, Occur.SHOULD);
        }
        
        if (q.getBroadcastWeighting() != 0.0f) {
            return new DistanceToBroadcastScore(query).withBroadcastWeight(q.getBroadcastWeighting());
        }
        return query;
    }
    
    private final static long MILLIS_IN_HOUR = Duration.standardHours(1).getMillis();
    
    private static int hourOf(long millis) {
        return (int) (millis / MILLIS_IN_HOUR);
    }
    
    private static int hourOf(Timestamp ts) {
        return hourOf(ts.millis());
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
            float broadcastScore = (float) (1f / (Math.abs(currentHour - broadcastHour) + 1));
            return subQueryScore  + (broadcastWeighting  * broadcastScore * subQueryScore);
        }
    }
    
    private void addPublisherfilter(TermsFilter filter, Set<Publisher> includedPublishers) {
        for (Publisher publisher : includedPublishers) {
            filter.addTerm(new Term(FIELD_CONTENT_PUBLISHER, publisher.toString()));
        }
    }
    
    private Query availabilityQuery(float boost) {
        
        TermsFilter filter = new TermsFilter();
        filter.addTerm(new Term(FIELD_AVAILABLE, TRUE));
        
        ConstantScoreQuery query = new ConstantScoreQuery(filter);
        query.setBoost(boost);
        return query;
    }
    
    private static Searcher searcherFor(Directory dir)  {
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
    
    private List<String> search(final Searcher searcher, Query query, Filter filter, Selection selection)  {
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
    

    @Override
    public void contentChange(Iterable<? extends Described> contents) {
        IndexWriter writer = null;
        try {
            writer = writerFor(contentDir);
            writer.setWriteLockTimeout(5000);
            for (Described content : Iterables.filter(contents, FILTER_SEARCHABLE_CONTENT)) {
                Document doc = asDocument(content);
                if (doc != null) {
                    writer.addDocument(doc);
                } else if (log.isInfoEnabled()) {
                    log.info("Content with title " + content.getTitle() + " and uri " + content.getCanonicalUri() + " not added due to null elements");
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (writer != null) {
                closeWriter(writer);
            }
        }
    }
    
    // Stop index from growing enormous
    private final static List<Publisher> VALID_PUBLISHERS = ImmutableList.of(Publisher.BBC, Publisher.C4, Publisher.FIVE, Publisher.PA, Publisher.ITV, Publisher.SEESAW, Publisher.ITUNES, Publisher.HULU, Publisher.HBO, Publisher.PREVIEW_NETWORKS);
    private final static Predicate<Described> FILTER_SEARCHABLE_CONTENT = new Predicate<Described>() {

        @Override
        public boolean apply(Described input) {
            if (input instanceof Item && (! VALID_PUBLISHERS.contains(input.getPublisher()) || hasContainer((Item) input))) {
                return false;
            }
            if (input instanceof ContentGroup && ! (input instanceof Person)) {
                return false;
            }

            return true;
        }
    };
    
    private final static boolean hasContainer(Item input) {
        return input.getContainer() != null;
    }

    public IndexStats stats() {
        return new IndexStats(ByteCount.bytes(contentDir.sizeInBytes()));
    }
    
    public static class IndexStats {

        private final ByteCount brandsIndexSize;

        public IndexStats(ByteCount brandsIndexSize) {
            this.brandsIndexSize = brandsIndexSize;
        }
        
        public ByteCount getBrandsIndexSize() {
            return brandsIndexSize;
        }

        public ByteCount getTotalIndexSize() {
            return brandsIndexSize;
        }
    }
}
