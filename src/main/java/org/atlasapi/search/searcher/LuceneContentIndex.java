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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanFilter;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterClause;
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
import org.apache.lucene.store.FSDirectory;
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
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.search.DebuggableContentSearcher;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
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
import org.apache.lucene.search.FilteredQuery;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Specialization;

public class LuceneContentIndex implements ContentChangeListener, DebuggableContentSearcher {
    
    private static final int MAX_RESULTS = 1000;
    private static final Logger log = LoggerFactory.getLogger(LuceneContentIndex.class);
    static final String FIELD_TITLE_FLATTENED = "title-flattened";
    static final String FIELD_CONTENT_TITLE = "title";
    static final String FIELD_CONTAINER_TITLE_FLATTENED = "container-title-flattened";
    static final String FIELD_CONTAINER_CONTENT_TITLE = "container-title";
    static final String FIELD_CONTENT_SPECIALIZATION = "specialization";
    static final String FIELD_CONTENT_PUBLISHER = "publisher";
    private static final String FIELD_CONTENT_URI = "contentUri";
    private static final String FIELD_AVAILABLE = "available";
    private static final String FIELD_BROADCAST_HOUR_TS = "broadcast";
    private static final String FIELD_CONTENT_IS_CONTAINER = "isContainer";
    private static final String FIELD_CONTENT_IS_TOP_LEVEL = "topLevel";
    private static final int HOURS_IN_A_WEEK = 168;
    private static final String TRUE = "T";
    private static final String FALSE = "F";
    private static final TitleQueryBuilder titleQueryBuilder = new TitleQueryBuilder();
    private static final Timestamper clock = new SystemClock();
    private final Directory contentDir;
    private final KnownTypeContentResolver contentResolver;
    private volatile Searcher contentSearcher;
    private Duration maxBroadcastAgeForInclusion = Duration.standardDays(365);
    private final IndexWriter indexWriter;
    
    public LuceneContentIndex(File luceneDir, KnownTypeContentResolver contentResolver) {
        this.contentResolver = contentResolver;
        try {
            this.contentDir = FSDirectory.open(luceneDir);
            this.indexWriter = new IndexWriter(contentDir, new StandardAnalyzer(Version.LUCENE_30), MaxFieldLength.UNLIMITED);
            touchIndex();
            indexWriter.setWriteLockTimeout(5000);
            this.contentSearcher = new IndexSearcher(contentDir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public SearchResults search(SearchQuery q) {
        return new SearchResults(search(getQuery(q), getFilter(q), q.getSelection()));
    }
    
    @Override
    public String debug(SearchQuery q) {
        return Joiner.on("\n").join(debug(getQuery(q), getFilter(q), q.getSelection()));
    }
    
    @Override
    public void beforeContentChange() {
    }
    
    @Override
    public void contentChange(Iterable<? extends Described> contents) {
        log.trace("Processing changed content");
        try {
            for (Described content : Iterables.filter(contents, FILTER_SEARCHABLE_CONTENT)) {
                try {
                    log.trace("Processing content {}", content.getCanonicalUri());
                    process(content);
                }
                catch (Exception e) {
                    log.error("Failed to index document " + content.getCanonicalUri(), e);
                }
            }
        } finally {
            commitWriter();
        }
    }
    
    @Override
    public void afterContentChange() {
        optimizeIndex();
        refreshSearcher();
    }
    
    private void touchIndex() {
        try {
            indexWriter.commit();
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }
    
    private void optimizeIndex()  {
        try {
            log.trace("Optimizing index");
            indexWriter.optimize();
            log.trace("Done optimizing index");
        } catch (Exception e) {
            Throwables.propagate(e);
        } finally {
            commitWriter();
        }
    }
    
    private void commitWriter() {
        try {
            log.trace("Commiting writer");
            indexWriter.commit();
            log.trace("Done commiting writer");
        } catch (Exception e) {
            Throwables.propagate(e);
        } 
    }
    
    private void process(Described content) throws CorruptIndexException, IOException {
        if(content instanceof Container) {
            Container container = (Container) content;
            List<LookupRef> lookupRefs = LookupRef.fromChildRefs(container.getChildRefs(), container.getPublisher());
            Iterable<Item> items = Iterables.filter(contentResolver.findByLookupRefs(lookupRefs).getAllResolvedResults(), Item.class);
            index(content, Optional.of(items), Optional.<Container>absent());
            
            for(Item child : items) {
                index(child, Optional.<Iterable<Item>>absent(), Optional.of(container));
            }
        } else {
            index(content, Optional.<Iterable<Item>>absent(), Optional.<Container>absent());
        }
    }
    
    private void index(Described content, Optional<Iterable<Item>> children, Optional<Container> parent) throws CorruptIndexException, IOException {
        Document doc = asDocument(content, children, parent);
        if (doc != null) {
            log.trace("Updating document");
            indexWriter.updateDocument(new Term(FIELD_CONTENT_URI, content.getCanonicalUri()), doc);
            log.trace("Done updating document");
        } else {
            log.info("Content with title {} and uri {} not added due to null elements", content.getTitle(), content.getCanonicalUri());
        }
    }
    
    private Document asDocument(Described content, Optional<Iterable<Item>> children, Optional<Container> parent) {
        if (Strings.isNullOrEmpty(content.getCanonicalUri()) || Strings.isNullOrEmpty(content.getTitle()) || content.getPublisher() == null) {
            return null;
        }
        Document doc = new Document();
        
        doc.add(new Field(FIELD_CONTENT_TITLE, content.getTitle(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_TITLE_FLATTENED, titleQueryBuilder.flatten(content.getTitle()), Field.Store.YES, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_CONTENT_URI, content.getCanonicalUri(), Field.Store.YES, Field.Index.NOT_ANALYZED));
        
        if(parent.isPresent()) {
            Container container = parent.get();
            if(!Strings.isNullOrEmpty(container.getTitle())) {
                doc.add(new Field(FIELD_CONTAINER_CONTENT_TITLE, container.getTitle(), Field.Store.NO, Field.Index.ANALYZED));
                doc.add(new Field(FIELD_CONTAINER_TITLE_FLATTENED, titleQueryBuilder.flatten(container.getTitle()), Field.Store.YES, Field.Index.ANALYZED));
            }
        }
        if (content.getSpecialization() != null) {
            doc.add(new Field(FIELD_CONTENT_SPECIALIZATION, content.getSpecialization().toString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
        }
        if (content.getPublisher() != null) {
            doc.add(new Field(FIELD_CONTENT_PUBLISHER, content.getPublisher().toString(), Field.Store.NO, Field.Index.NOT_ANALYZED));
        }
        
        boolean availabilityFieldsAdded = false;
        if(content instanceof Item) {
            availabilityFieldsAdded = addBroadcastAndAvailabilityFields((Item)content, doc);
        } else if (content instanceof Song){
            availabilityFieldsAdded = addBroadcastAndAvailabilityFields((Song)content, doc);
        } else if (content instanceof Container) {
            availabilityFieldsAdded = addBroadcastAndAvailabilityFields((Container)content, children.get(), doc);
        }
        
        if(!availabilityFieldsAdded) {
            return null;
        }
        
        boolean container = content instanceof Container;
        doc.add(new Field(FIELD_CONTENT_IS_CONTAINER, container ? TRUE : FALSE, Field.Store.NO, Field.Index.NOT_ANALYZED));
        boolean topLevel = true;
        if (content instanceof Item && ((Item)content).getContainer() != null) {
            topLevel = false;
        } else if (content instanceof Series && ((Series)content).getParent() != null) {
            topLevel = false;
        }
        doc.add(new Field(FIELD_CONTENT_IS_TOP_LEVEL, topLevel ? TRUE : FALSE, Field.Store.NO, Field.Index.NOT_ANALYZED));
        return doc;
    }
    
    private boolean addBroadcastAndAvailabilityFields(Song song, Document doc) {
        return true;
    }
    
    private boolean addBroadcastAndAvailabilityFields(Item item, Document doc) {
        log.trace("Adding broadcast and availability fields for item {}", item.getCanonicalUri());
        Timestamp now = clock.timestamp();

        if (item.isAvailable()) {
            doc.add(new Field(FIELD_AVAILABLE, TRUE, Field.Store.NO, Field.Index.NOT_ANALYZED));
        }
        int hourOfClosestBroadcast = hourOfClosestBroadcast(item.flattenBroadcasts(), now);
        
        if (item instanceof Film) {
            // Films should pretend to be at most 30 days old 
            hourOfClosestBroadcast = Math.max(hourOf(now.minus(Duration.standardDays(30))), hourOfClosestBroadcast);
        }
        
        doc.add(new NumericField(FIELD_BROADCAST_HOUR_TS, Field.Store.YES, true).setIntValue(hourOfClosestBroadcast));
        return true;
    }
    
    private boolean addBroadcastAndAvailabilityFields(Container container, Iterable<Item> children, Document doc) {
        log.trace("Adding broadcast and availability fields for container {}", container.getCanonicalUri());
        Timestamp now = clock.timestamp();
        if (!container.getChildRefs().isEmpty()) {

            if (haveAvailable(children)) {
                doc.add(new Field(FIELD_AVAILABLE, TRUE, Field.Store.NO, Field.Index.NOT_ANALYZED));
            }
            
            int hourOfClosestBroadcastForItems = hourOfClosestBroadcastForItems(children, now);
            doc.add(new NumericField(FIELD_BROADCAST_HOUR_TS, Field.Store.YES, true).setIntValue(hourOfClosestBroadcastForItems));
            return true;
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
        Iterable<Broadcast> publishedBroadcasts = Iterables.filter(broadcasts, new Predicate<Broadcast>() {
            
            @Override
            public boolean apply(Broadcast input) {
                return input.isActivelyPublished();
            }
        });
        
        if (Iterables.isEmpty(publishedBroadcasts)) {
            return 0;
        }
        
        Broadcast closest = sinceBroadcast(now).min(publishedBroadcasts);
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
    
    private Filter getFilter(SearchQuery q) {
        return getPublisherFilter(q.getIncludedPublishers());
    }
    
    private Query getQuery(SearchQuery q) {
        BooleanQuery query = new BooleanQuery(true);
        // Title:
        Query titleQuery = titleQueryBuilder.build(q.getTerm());
        titleQuery.setBoost(q.getTitleWeighting());
        // Apply filters
        Optional<BooleanFilter> filter = filtersFor(q);
        if(filter.isPresent()) {
            titleQuery = new FilteredQuery(titleQuery, filter.get());
        }
        query.add(titleQuery, Occur.MUST);
        // Availability:
        if (q.getCatchupWeighting() != 0.0f) {
            Query availabilityQuery = availabilityQuery(q.getCatchupWeighting());
            query.add(availabilityQuery, Occur.SHOULD);
        }
        // Result:
        if (q.getBroadcastWeighting() != 0.0f) {
            return new DistanceToBroadcastScore(query).withBroadcastWeight(q.getBroadcastWeighting());
        } else {
            return query;
        }
    }

    private Optional<BooleanFilter> filtersFor(SearchQuery q) {
        List<FilterClause> filters = Lists.newArrayList();
        
        if (!q.getIncludedSpecializations().isEmpty()) {
            filters.add(new FilterClause(getSpecializationFilter(q.getIncludedSpecializations()), Occur.MUST));            
        }
        if ("item".equals(q.type())) {
            TermsFilter typeField = new TermsFilter();
            typeField.addTerm(new Term(FIELD_CONTENT_IS_CONTAINER, FALSE));
            filters.add(new FilterClause(typeField, Occur.MUST));
        } else if ("container".equals(q.type())) {
            TermsFilter typeField = new TermsFilter();
            typeField.addTerm(new Term(FIELD_CONTENT_IS_CONTAINER, TRUE));
            filters.add(new FilterClause(typeField, Occur.MUST));
        }
        if (q.topLevelOnly() != null && q.topLevelOnly()) {
            TermsFilter typeField = new TermsFilter();
            typeField.addTerm(new Term(FIELD_CONTENT_IS_TOP_LEVEL, TRUE));
            filters.add(new FilterClause(typeField, Occur.MUST));
        }
        if(filters.isEmpty()) {
            return Optional.absent();
        }
        
        BooleanFilter f = new BooleanFilter();
        for(FilterClause filter : filters) {
            f.add(filter);
        }
        return Optional.of(f);
    }

    private final static long MILLIS_IN_HOUR = Duration.standardHours(1).getMillis();
    
    private static int hourOf(long millis) {
        return (int) (millis / MILLIS_IN_HOUR);
    }
    
    private static int hourOf(Timestamp ts) {
        return hourOf(ts.millis());
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
        filter.addTerm(new Term(FIELD_AVAILABLE, TRUE));
        
        ConstantScoreQuery query = new ConstantScoreQuery(filter);
        query.setBoost(boost);
        return query;
    }
    
    private void refreshSearcher() {
        Exception error = null;
        try {
            this.contentSearcher.close();
        } catch (Exception ex) {
            error = ex;
        } finally {
            // Refresh the searcher in any case:
            try {
                this.contentSearcher = new IndexSearcher(contentDir);
            } catch (IOException ex) {
                // An error in refreshing the searcher is more important than an error in closing it:
                error =  ex;
            }
            // If there was an error, propagate it:
            if (error != null) {
                throw new RuntimeException(error);
            }
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
    
    private List<String> search(Query query, Filter filter, Selection selection) {
        try {
            /*
             * We re-sort the results so that when two items have the same score
             * the item with the shortest title wins.
             */
            TopDocs topDocs = getTopDocs(query, filter, selection);
            List<Result> results = Lists.newArrayList();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = contentSearcher.doc(scoreDoc.doc);
                results.add(new Result(scoreDoc, doc));
            }
            Collections.sort(results);
            return Lists.transform(results, TO_URI);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private List<String> debug(Query query, Filter filter, Selection selection) {
        try {
            TopDocs topDocs = getTopDocs(query, filter, selection);
            
            List<String> results = Lists.newArrayList();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                Document doc = contentSearcher.doc(scoreDoc.doc);
                results.add(doc.getField(FIELD_CONTENT_URI).stringValue() + " : " + scoreDoc.score + "\n" + contentSearcher.explain(query.weight(contentSearcher), scoreDoc.doc));
            }
            return results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    private TopDocs getTopDocs(Query query, Filter filter, Selection selection) throws IOException {
        int startIndex = selection.getOffset();
        int endIndex = selection.limitOrDefaultValue(MAX_RESULTS);
        
        TopScoreDocCollector collector = TopScoreDocCollector.create(MAX_RESULTS, true);
        
        contentSearcher.search(query.weight(contentSearcher), filter, collector);
        
        return collector.topDocs(startIndex, endIndex);
    }

    private final static Predicate<Described> FILTER_SEARCHABLE_CONTENT = new Predicate<Described>() {

        @Override
        public boolean apply(Described input) {
            if (input instanceof ContentGroup && !(input instanceof Person)) {
                return false;
            } else {
                return true;
            }
        }
    };
    
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
            return subQueryScore + (broadcastWeighting * broadcastScore * subQueryScore);
        }
    }
}
