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

import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.PeopleLister;
import org.atlasapi.persistence.content.PeopleListerListener;

import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingHandler;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.search.searcher.ContentChangeListener;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class MongoDbBackedContentBootstrapper {
	
    private static final Log log = LogFactory.getLog(MongoDbBackedContentBootstrapper.class);

    private final ContentLister contentLister;
    private PeopleLister peopleLister;
    
    public MongoDbBackedContentBootstrapper(ContentLister contentLister) {
        this.contentLister = contentLister;
    }
    
    public MongoDbBackedContentBootstrapper withPeopleLister(PeopleLister peopleLister) {
        this.peopleLister = peopleLister;
        return this;
    }
    
	public void loadAllIntoListener(final ContentChangeListener listener) {
	    if (log.isInfoEnabled()) {
            log.info("Bootstrapping top level content");
        }
	    
		final AtomicInteger numberProcessed = new AtomicInteger(0);
		
        ContentListingHandler handler = new ContentListingHandler() {

            @Override
            public boolean handle(Iterable<? extends Content> contents, ContentListingProgress progress) {
                listener.contentChange(contents);
                numberProcessed.incrementAndGet();
                if(numberProcessed.incrementAndGet() % 500 == 0) {
                    log.info(progress.toString());
                }
                return true; 
            }
        };
        
        contentLister.listContent(ImmutableSet.of(TOP_LEVEL_CONTAINERS, TOP_LEVEL_ITEMS), ContentListingCriteria.defaultCriteria(), handler);
		
        if(peopleLister != null) {
		    peopleLister.list(new PeopleListerListener() {
                @Override
                public void personListed(Person person) {
                    listener.contentChange(ImmutableList.of(person));
                    numberProcessed.incrementAndGet();
                }
            });
		}
		
		if (log.isInfoEnabled()) {
		    log.info("Passed "+numberProcessed+" to content change listener");
		}
	}
}
