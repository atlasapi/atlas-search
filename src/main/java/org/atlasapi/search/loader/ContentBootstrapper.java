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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.PeopleLister;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.listing.ProgressStore;
import org.atlasapi.search.searcher.ContentChangeListener;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

public class ContentBootstrapper {

    private static final Log log = LogFactory.getLog(ContentBootstrapper.class);

    private final ContentListingCriteria.Builder criteriaBuilder;
    private final ProgressStore progressStore;
    private final String taskName;
    private final ContentLister contentLister;
    private final Optional<PeopleLister> peopleLister;

    private ContentBootstrapper(
            ContentListingCriteria.Builder criteriaBuilder,
            ProgressStore progressStore,
            String taskName,
            ContentLister contentLister,
            Optional<PeopleLister> peopleLister
    ) {
        this.criteriaBuilder = checkNotNull(criteriaBuilder);
        this.progressStore = checkNotNull(progressStore);
        this.taskName = checkNotNull(taskName);
        this.contentLister = checkNotNull(contentLister);
        this.peopleLister = checkNotNull(peopleLister);
    }

    public static TaskStep builder() {
        return new Builder();
    }

    public void loadAllIntoListener(final ContentChangeListener listener) {
        log.info("Loading content into listener for task " + taskName);
        listener.beforeContentChange();
        try {
            bootstrapPeople(listener);
            bootstrapContent(listener);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            listener.afterContentChange();
        }
    }

    private void bootstrapPeople(ContentChangeListener listener) {
        log.info("Bootstrapping people.");

        final AtomicInteger peopleProcessed = new AtomicInteger(0);
        if (peopleLister.isPresent()) {
            peopleLister.get().list(person -> {
                try {
                    listener.contentChange(ImmutableList.of(person));
                    peopleProcessed.incrementAndGet();
                } catch (RuntimeException ex) {
                    log.warn("Failed to index person " + person.getCanonicalUri(), ex);
                }
            });
        }

        log.info(String.format("Finished bootstrapping %s people", peopleProcessed.get()));
    }

    private void bootstrapContent(ContentChangeListener listener) {
        log.info("Bootstrapping top level content");

        int contentProcessed = 0;
        Iterator<Content> content = contentLister.listContent(getCriteria());
        Iterator<List<Content>> partitionedContent = Iterators.partition(content, 100);

        while (partitionedContent.hasNext()) {
            try {
                Iterable<Content> partition = partitionedContent.next()
                        .stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                listener.contentChange(partition);

                contentProcessed += Iterables.size(partition);
                ContentListingProgress progress = ContentListingProgress.progressFrom(
                        Iterables.getLast(partition)
                );
                progressStore.storeProgress(taskName, progress);

                log.info(String.format("%s content processed: %s", contentProcessed, progress));
            } catch (Exception e) {
                log.error("Failed to process partition, continuing to next", e);
            }
        }
        log.info(String.format("Finished bootstrapping %s content.", contentProcessed));
    }

    private ContentListingCriteria getCriteria() {
        Optional<ContentListingProgress> progressOptional = progressStore.progressForTask(taskName);

        ContentListingCriteria criteria;
        if (progressOptional.isPresent()) {
            ContentListingProgress progress = progressOptional.get();
            criteria = criteriaBuilder
                    .startingAt(progress)
                    .build();

            log.info("Found existing progress for " + taskName + " bootstrap. "
                    + "Resuming from " + progress.getUri());
        } else {
            criteria = criteriaBuilder.build();

            log.info("Found no existing progress for " + taskName + " bootstrap.");
        }
        return criteria;
    }

    public interface TaskStep {
        ProgressStoreStep withTaskName(String taskName);
    }

    public interface ProgressStoreStep {
        ContentListerStep withProgressStore(ProgressStore progressStore);
    }

    public interface ContentListerStep {
        BuildStep withContentLister(ContentLister contentLister);
    }

    public interface BuildStep {
        BuildStep withCriteriaBuilder(ContentListingCriteria.Builder criteriaBuilder);
        BuildStep withPeopleLister(PeopleLister peopleLister);
        ContentBootstrapper build();
    }

    private static class Builder implements TaskStep, ProgressStoreStep, ContentListerStep,
            BuildStep {

        private String taskName;
        private ProgressStore progressStore;
        private ContentLister contentLister;

        private ContentListingCriteria.Builder criteriaBuilder  = defaultCriteria()
                .forContent(ImmutableSet.of(
                        ContentCategory.CONTAINER, ContentCategory.TOP_LEVEL_ITEM
                ));
        private Optional<PeopleLister> peopleLister = Optional.absent();

        @Override
        public ProgressStoreStep withTaskName(String taskName) {
            this.taskName = taskName;
            return this;
        }

        @Override
        public ContentListerStep withProgressStore(ProgressStore progressStore) {
            this.progressStore = progressStore;
            return this;
        }

        @Override
        public BuildStep withContentLister(ContentLister contentLister) {
            this.contentLister = contentLister;
            return this;
        }

        @Override
        public BuildStep withCriteriaBuilder(ContentListingCriteria.Builder criteriaBuilder) {
            this.criteriaBuilder = criteriaBuilder;
            return this;
        }

        @Override
        public BuildStep withPeopleLister(PeopleLister peopleLister) {
            this.peopleLister = Optional.of(peopleLister);
            return this;
        }

        @Override
        public ContentBootstrapper build() {
            return new ContentBootstrapper(
                    criteriaBuilder,
                    progressStore,
                    taskName,
                    contentLister,
                    peopleLister
            );
        }
    }
}
