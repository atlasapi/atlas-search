/* Copyright 2009 Meta Broadcast Ltd

 Licensed under the Apache License, Version 2.0 (the "License"); you
 may not use this file except in compliance with the License. You may
 obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License. */
package org.atlasapi.search.loader;

import static com.google.common.base.Predicates.notNull;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.PeopleLister;
import org.atlasapi.persistence.content.PeopleListerListener;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.search.searcher.ContentChangeListener;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

public class ContentBootstrapper {

    private static final Log log = LogFactory.getLog(ContentBootstrapper.class);
    private ContentLister[] contentListers;
    private PeopleLister[] peopleListers;

    public ContentBootstrapper withContentListers(ContentLister... contentListers) {
        this.contentListers = contentListers;
        return this;
    }

    public ContentBootstrapper withPeopleListers(PeopleLister... peopleListers) {
        this.peopleListers = peopleListers;
        return this;
    }

    public void loadAllIntoListener(final ContentChangeListener listener) {
        if (log.isInfoEnabled()) {
            log.info("Bootstrapping top level content");
        }

        int contentProcessed = 0;
        for (ContentLister lister : contentListers) {
            try {
                Iterator<Content> content = lister.listContent(defaultCriteria().forContent(ImmutableList.copyOf(ContentCategory.TOP_LEVEL_CONTENT)).build());
                Iterator<List<Content>> partitionedContent = Iterators.paddedPartition(content, 100);
                while (partitionedContent.hasNext()) {
                    List<Content> partition = ImmutableList.copyOf(Iterables.filter(partitionedContent.next(), notNull()));
                    listener.contentChange(partition);
                    contentProcessed += partition.size();
                    if (log.isInfoEnabled()) {
                        log.info(String.format("%s content processed: %s", contentProcessed, ContentListingProgress.progressFrom(Iterables.getLast(partition))));
                    }
                }
            } catch (Exception ex) {
                log.warn(ex.getMessage(), ex);
            }
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("Finished bootstrapping %s content.", contentProcessed));
            log.info("Bootstrapping people.");
        }

        final AtomicInteger peopleProcessed = new AtomicInteger(0);
        if (peopleListers != null) {
            for (PeopleLister lister : peopleListers) {
                lister.list(new PeopleListerListener() {

                    @Override
                    public void personListed(Person person) {
                        try {
                            listener.contentChange(ImmutableList.of(person));
                            peopleProcessed.incrementAndGet();
                        } catch (RuntimeException ex) {
                            log.warn(ex.getMessage(), ex);
                            throw ex;
                        }
                    }
                });
            }
        }

        if (log.isInfoEnabled()) {
            log.info(String.format("Finished bootstrapping %s people", peopleProcessed.get()));
            log.info("Passed " + (contentProcessed + peopleProcessed.get()) + " to content change listener");
        }
    }
}