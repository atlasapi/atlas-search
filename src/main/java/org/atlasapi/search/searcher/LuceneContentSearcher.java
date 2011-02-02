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
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Version;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.search.model.SearchResults;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metabroadcast.common.file.MoreFiles;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stats.Score;
import com.metabroadcast.common.units.ByteCount;

public class LuceneContentSearcher implements ContentListener, ContentSearcher {

	private static final Log log = LogFactory.getLog(LuceneContentSearcher.class);
	
	static final String FIELD_TITLE_FLATTENED = "title-flattened";
	static final String FIELD_CONTENT_TITLE = "title";
	static final String FIELD_CONTENT_PUBLISHER = "publisher";
	private static final String FIELD_CONTENT_URI = "contentUri";
	
	private static final TitleQueryBuilder titleQueryBuilder = new TitleQueryBuilder();

	protected static final int MAX_RESULTS = 5000;
	
	private final RAMDirectory brandsDir = new RAMDirectory();
	private final File itemsDirFile = Files.createTempDir();
	private final SimpleFSDirectory itemsDir = goAheadMakeMyDir(itemsDirFile);

	public LuceneContentSearcher() {
		try {
			formatDirectory(brandsDir);
			formatDirectory(itemsDir);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private SimpleFSDirectory goAheadMakeMyDir(File itemsDirFile) {
	    try {
            return new SimpleFSDirectory(itemsDirFile, new SingleInstanceLockFactory());
        } catch (IOException e) {
            throw new RuntimeException("Unable to setup FS directory for lucene", e);
        }
	}
	
	private static void closeWriter(IndexWriter writer) {
		try {
			//writer.commit();
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


	private Document asDocument(Content content) {
		if (Strings.isNullOrEmpty(content.getCanonicalUri()) || Strings.isNullOrEmpty(content.getTitle()) || content.getPublisher() == null) {
			return null;
		}
		Document doc = new Document();
        doc.add(new Field(FIELD_CONTENT_TITLE, content.getTitle(), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_TITLE_FLATTENED, titleQueryBuilder.flatten(content.getTitle()), Field.Store.NO, Field.Index.ANALYZED));
        doc.add(new Field(FIELD_CONTENT_URI, content.getCanonicalUri(), Field.Store.YES,  Field.Index.NOT_ANALYZED));
        doc.add(new Field(FIELD_CONTENT_PUBLISHER, content.getPublisher().toString(), Field.Store.NO,  Field.Index.NOT_ANALYZED));
        return doc;
	}
	
	@Override
	public SearchResults search(SearchQuery q) {
		BooleanQuery titleAndPublisher = new BooleanQuery();
		titleAndPublisher.add(titleQueryBuilder.build(q.getTerm()), Occur.MUST);
		titleAndPublisher.add(publisherQuery(q.getIncludedPublishers()), Occur.MUST);
		return new SearchResults(search(searcherFor(brandsDir), titleAndPublisher, q.getSelection()));
	}

	private Query publisherQuery(Set<Publisher> includedPublishers) {
		BooleanQuery publisherQuery = new BooleanQuery();
		for (Publisher publisher : includedPublishers) {
			publisherQuery.add(new TermQuery(new Term(FIELD_CONTENT_PUBLISHER, publisher.toString())), Occur.SHOULD);
		}
		return publisherQuery;

	}

	public List<String> itemTitleSearch(String queryString, Selection selection) {
		return search(searcherFor(itemsDir), titleQueryBuilder.build(queryString), selection);
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
	public void brandChanged(Iterable<? extends Container<?>> brands, ChangeType changeType) {
		IndexWriter writer = null;
		try {
			writer = writerFor(brandsDir);
			writer.setWriteLockTimeout(5000);
			for (Container<?> brand : brands) {
				Document doc = asDocument(brand);
				if (doc != null) {
					if (changeType == ContentListener.ChangeType.BOOTSTRAP) {
						writer.addDocument(doc);
					}
					else {
						writer.updateDocument(new Term(FIELD_CONTENT_URI, brand.getCanonicalUri()), doc);	
					}
				}
				else {
					log.info("Content with title " + brand.getTitle() + " and uri " + brand.getCanonicalUri() + " not added due to null elements");
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}		
		finally {
			if (writer != null) {
				closeWriter(writer);
			}
		}
	}

	@Override
	public void itemChanged(Iterable<? extends Item> items, ChangeType changeType) {
		IndexWriter writer = null;
		try {
			writer = writerFor(itemsDir);
			writer.setWriteLockTimeout(5000);
			for (Item item : items) {
				Document doc = asDocument(item);
				if (doc != null) {
					if (changeType == ContentListener.ChangeType.BOOTSTRAP) {
						writer.addDocument(doc);
					}
					else {
						writer.updateDocument(new Term(FIELD_CONTENT_URI, item.getCanonicalUri()), doc);	
					}
				}
				else {
					log.info("Item with title " + item.getTitle() + " and uri " + item.getCanonicalUri() + " not added due to null elements");
				}
			}
		} catch(Exception e) {
			throw new RuntimeException(e);
		}		
		finally {
			if (writer != null) {
				closeWriter(writer);
			}
		}
	}

	public IndexStats stats() {
		return new IndexStats(ByteCount.bytes(brandsDir.sizeInBytes()), MoreFiles.size(itemsDirFile));
	}
	
	public static class IndexStats {

		private final ByteCount brandsIndexSize;
		private final ByteCount itemsIndexSize;

		public IndexStats(ByteCount brandsIndexSize, ByteCount itemsIndexSize) {
			this.brandsIndexSize = brandsIndexSize;
			this.itemsIndexSize = itemsIndexSize;
		}
		
		public ByteCount getBrandsIndexSize() {
			return brandsIndexSize;
		}
		
		public ByteCount getItemsIndexSize() {
			return itemsIndexSize;
		}

		public ByteCount getTotalIndexSize() {
			return brandsIndexSize.plus(itemsIndexSize);
		}
	}

	
}
