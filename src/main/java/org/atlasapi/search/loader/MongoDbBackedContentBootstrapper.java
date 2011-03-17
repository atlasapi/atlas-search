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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.content.RetrospectiveContentLister;

import com.google.common.collect.Iterables;

public class MongoDbBackedContentBootstrapper {
	
    private static final Log log = LogFactory.getLog(MongoDbBackedContentBootstrapper.class);
    private static final int BATCH_SIZE = 100;

    private final RetrospectiveContentLister contentStore;
    private int batchSize = BATCH_SIZE;

    public MongoDbBackedContentBootstrapper(RetrospectiveContentLister contentLister) {
        this.contentStore = contentLister;
    }
    
	@SuppressWarnings("unchecked")
	public void loadAllIntoListener(ContentListener listener) {
	    if (log.isInfoEnabled()) {
            log.info("Bootstrapping top level content");
        }
	    
		String fromId = null;
		while (true) {
			List<Content> roots = contentStore.listAllRoots(fromId, -batchSize);
            if (roots.isEmpty()) {
                break;
            }
			
			Iterable<Item> items = Iterables.filter(roots, Item.class);
			if (!Iterables.isEmpty(items)) {
			    listener.itemChanged(items, ContentListener.ChangeType.BOOTSTRAP);
			}
			Iterable containers = (Iterable) Iterables.filter(roots, Container.class);
			if (!Iterables.isEmpty(containers)) {
			    listener.brandChanged(containers, ContentListener.ChangeType.BOOTSTRAP);
			}
			
			Content last = Iterables.getLast(roots);
			fromId = last.getCanonicalUri();
		}
	}

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
