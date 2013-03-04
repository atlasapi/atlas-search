package org.atlasapi.search.searcher;

import java.math.BigInteger;
import java.util.Set;
import java.util.concurrent.Callable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.channel.ChannelGroupResolver;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.caching.BackgroundComputingValue;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

public class ChannelGroupBroadcastChannelBooster implements BroadcastBooster {

    private static final Logger log = LoggerFactory.getLogger(ChannelGroupBroadcastChannelBooster.class);
    
    private final ChannelResolver channelResolver;
    private final BackgroundComputingValue<Set<String>> priorityChannels;
    private final SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec();

    public ChannelGroupBroadcastChannelBooster(final ChannelGroupResolver channelGroupResolver, ChannelResolver channelResolver, String channelGroup) {
        final BigInteger channelGroupId = codec.decode(channelGroup);
        this.channelResolver = Preconditions.checkNotNull(channelResolver);
        this.priorityChannels = new BackgroundComputingValue<Set<String>>(Duration.standardHours(1), new Callable<Set<String>>() {

            @Override
            public Set<String> call() throws Exception {
                Optional<ChannelGroup> channelGroup = channelGroupResolver.channelGroupFor(channelGroupId.longValue());
                return getCurrentChannelsInGroup(channelGroup.get());
            }
            
        });
        
        priorityChannels.start();
    }
    
    private Set<String> getCurrentChannelsInGroup(ChannelGroup channelGroup) {

        LocalDate today = new LocalDate(DateTimeZone.UTC);
        Builder<String> channelUris = ImmutableSet.<String>builder();
        for(ChannelNumbering numbering: channelGroup.getChannelNumberings()) {
            if( (numbering.getStartDate() == null || numbering.getStartDate().isBefore(today)) 
                    && (numbering.getEndDate() == null || numbering.getEndDate().isAfter(today))) {
                
                Maybe<Channel> maybeChannel = channelResolver.fromId(numbering.getChannel());
                if(maybeChannel.hasValue()) {
                    channelUris.add(maybeChannel.requireValue().getCanonicalUri());
                } else {
                    log.error("Could not find channel ID " + numbering.getChannel());
                }
            }
        }
        return channelUris.build();
    };
    
    @Override
    public boolean shouldBoost(Broadcast broadcast) {
        return broadcast.getTransmissionEndTime().isAfterNow() && priorityChannels.get().contains(broadcast.getBroadcastOn());
    }
}
