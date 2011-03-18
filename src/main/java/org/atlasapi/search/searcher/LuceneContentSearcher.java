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
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.search.model.SearchResults;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stats.Score;
import com.metabroadcast.common.units.ByteCount;

public class LuceneContentSearcher implements ContentChangeListener, ContentSearcher {

	private static final Log log = LogFactory.getLog(LuceneContentSearcher.class);
	
	static final String FIELD_TITLE_FLATTENED = "title-flattened";
	static final String FIELD_CONTENT_TITLE = "title";
	static final String FIELD_CONTENT_PUBLISHER = "publisher";
	private static final String FIELD_CONTENT_URI = "contentUri";
	
	private static final TitleQueryBuilder titleQueryBuilder = new TitleQueryBuilder();

	protected static final int MAX_RESULTS = 5000;
	
	private final RAMDirectory contentDir = new RAMDirectory();

	public LuceneContentSearcher() {
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
        doc.add(new Field(FIELD_TITLE_FLATTENED, titleQueryBuilder.flatten(content.getTitle()), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_CONTENT_URI, content.getCanonicalUri(), Field.Store.YES,  Field.Index.NOT_ANALYZED));
        doc.add(new Field(FIELD_CONTENT_PUBLISHER, content.getPublisher().toString(), Field.Store.NO,  Field.Index.NOT_ANALYZED));
        // TODO: add availablility as another ordering
        return doc;
	}
	
	@Override
	public SearchResults search(SearchQuery q) {
		BooleanQuery titleAndPublisher = new BooleanQuery();
		titleAndPublisher.add(titleQueryBuilder.build(q.getTerm()), Occur.MUST);
		titleAndPublisher.add(publisherQuery(q.getIncludedPublishers()), Occur.MUST);
		return new SearchResults(search(searcherFor(contentDir), titleAndPublisher, q.getSelection()));
	}

	private Query publisherQuery(Set<Publisher> includedPublishers) {
		BooleanQuery publisherQuery = new BooleanQuery();
		for (Publisher publisher : includedPublishers) {
			publisherQuery.add(new TermQuery(new Term(FIELD_CONTENT_PUBLISHER, publisher.toString())), Occur.SHOULD);
		}
		return publisherQuery;

	}
	
	private static Searcher searcherFor(Directory dir)  {
		try {
			return new IndexSearcher(dir);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private List<String> search(Searcher searcher, Query query, Selection selection)  {
		try {
			int startIndex = selection.getOffset();
			int endIndex = selection.hasLimit() ? startIndex + selection.getLimit() : Integer.MAX_VALUE;
			
			final List<Score<Integer>> hits = Lists.newArrayList();
			
			searcher.search(query, new Collector() {
				
				private Scorer scorer;

				@Override
				public void setScorer(Scorer scorer) throws IOException {
					this.scorer = scorer;
				}
				
				@Override
				public void setNextReader(IndexReader arg0, int docBase) throws IOException {
				}
				
				@Override
				public void collect(int docId) throws IOException {
					if (hits.size() < MAX_RESULTS) {
						hits.add(new Score<Integer>(docId, scorer.score()));
					}
				}
				
				@Override
				public boolean acceptsDocsOutOfOrder() {
					return false;
				}
			});
			
			Collections.sort(hits, Collections.reverseOrder());
			
			List<String> found = Lists.newArrayListWithCapacity(hits.size());

			for (int i = startIndex; i < Math.min(hits.size(), endIndex); i++) {
				Document doc = searcher.doc(hits.get(i).getTarget());
				found.add(doc.getField(FIELD_CONTENT_URI).stringValue());
			}
			return found;
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
    private final static List<Publisher> VALID_PUBLISHERS = ImmutableList.of(Publisher.BBC, Publisher.C4, Publisher.FIVE, Publisher.PA, Publisher.ITV, Publisher.SEESAW, Publisher.ITUNES, Publisher.HULU, Publisher.HBO);
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
