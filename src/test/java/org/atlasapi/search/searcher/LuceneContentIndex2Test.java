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

import com.google.common.collect.ImmutableList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.DummyKnownTypeContentResolver;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.query.Selection;
import java.io.File;

public class LuceneContentIndex2Test extends TestCase {

    private static final ImmutableSet<Publisher> ALL_PUBLISHERS = ImmutableSet.copyOf(Publisher.values());
    //
    private DummyKnownTypeContentResolver contentResolver;
    private LuceneContentIndex searcher;

    @Override
    protected void setUp() throws Exception {
        contentResolver = new DummyKnownTypeContentResolver();
        searcher = new LuceneContentIndex(new File("/Users/sergio/Dev/Lab/metabroadcast/atlas-search/mnt/data/lucene"), contentResolver);
    }

    public void testFast() throws Exception {
        for (int i = 0; i < 100; i++) {
            searcher.search(title("Eastenders"));
        }
        long start = System.currentTimeMillis();
        assertEquals(12, searcher.search(title("Eastenders")).toUris().size());
        System.out.println("Elapsed for fast: " + (System.currentTimeMillis() - start));
    }

    public void testSlow() throws Exception {
        for (int i = 0; i < 100; i++) {
            searcher.search(title("Life On Mars"));
        }
        long start = System.currentTimeMillis();
        searcher.search(title("Life On Mars"));
        System.out.println("Elapsed for slow: " + (System.currentTimeMillis() - start));
    }

    protected static Brand brand(String uri, String title) {
        Brand b = new Brand(uri, uri, Publisher.BBC);
        b.setTitle(title);
        return b;
    }

    protected static SearchQuery title(String term) {
        return new SearchQuery(term, Selection.ALL, ImmutableList.of(Publisher.PA), 1.0f, 1.0f, 0.0f);
    }

    protected static void check(SearchResults result, Identified... content) {
        assertThat(result.toUris(), is(toUris(Arrays.asList(content))));
    }

    private static List<String> toUris(List<? extends Identified> content) {
        List<String> uris = Lists.newArrayList();
        for (Identified description : content) {
            uris.add(description.getCanonicalUri());
        }
        return uris;
    }
}
