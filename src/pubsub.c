/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 */

#include "server.h"
#include "cluster.h"
#include "trie.h"

/* Structure to hold the pubsub related metadata. Currently used
 * for pubsub and pubsubshard feature. */
typedef struct pubsubtype {
    int shard;
    dict *(*clientPubSubChannels)(client*);
    int (*subscriptionCount)(client*);
    kvstore **serverPubSubChannels;
    robj **subscribeMsg;
    robj **unsubscribeMsg;
    robj **messageBulk;
}pubsubtype;

/*
 * Get client's global Pub/Sub channels subscription count.
 */
int clientSubscriptionsCount(client *c);

/*
 * Get client's shard level Pub/Sub channels subscription count.
 */
int clientShardSubscriptionsCount(client *c);

/*
 * Get client's global Pub/Sub channels dict.
 */
dict* getClientPubSubChannels(client *c);

/*
 * Get client's shard level Pub/Sub channels dict.
 */
dict* getClientPubSubShardChannels(client *c);

/*
 * Get list of channels client is subscribed to.
 * If a pattern is provided, the subset of channels is returned
 * matching the pattern.
 */
void channelList(client *c, sds pat, kvstore *pubsub_channels);

/*
 * Pub/Sub type for global channels.
 */
pubsubtype pubSubType = {
    .shard = 0,
    .clientPubSubChannels = getClientPubSubChannels,
    .subscriptionCount = clientSubscriptionsCount,
    .serverPubSubChannels = &server.pubsub_channels,
    .subscribeMsg = &shared.subscribebulk,
    .unsubscribeMsg = &shared.unsubscribebulk,
    .messageBulk = &shared.messagebulk,
};

/*
 * Pub/Sub type for shard level channels bounded to a slot.
 */
pubsubtype pubSubShardType = {
    .shard = 1,
    .clientPubSubChannels = getClientPubSubShardChannels,
    .subscriptionCount = clientShardSubscriptionsCount,
    .serverPubSubChannels = &server.pubsubshard_channels,
    .subscribeMsg = &shared.ssubscribebulk,
    .unsubscribeMsg = &shared.sunsubscribebulk,
    .messageBulk = &shared.smessagebulk,
};

/*-----------------------------------------------------------------------------
 * Pubsub client replies API
 *----------------------------------------------------------------------------*/

/* Send a pubsub message of type "message" to the client.
 * Normally 'msg' is a Redis object containing the string to send as
 * message. However if the caller sets 'msg' as NULL, it will be able
 * to send a special message (for instance an Array type) by using the
 * addReply*() API family. */
void addReplyPubsubMessage(client *c, robj *channel, robj *msg, robj *message_bulk) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,message_bulk);
    addReplyBulk(c,channel);
    if (msg) addReplyBulk(c,msg);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send a pubsub message of type "pmessage" to the client. The difference
 * with the "message" type delivered by addReplyPubsubMessage() is that
 * this message format also includes the pattern that matched the message. */
void addReplyPubsubPatMessage(client *c, robj *pat, robj *channel, robj *msg) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[4]);
    else
        addReplyPushLen(c,4);
    addReply(c,shared.pmessagebulk);
    addReplyBulk(c,pat);
    addReplyBulk(c,channel);
    addReplyBulk(c,msg);
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub subscription notification to the client. */
void addReplyPubsubSubscribed(client *c, robj *channel, pubsubtype type) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,*type.subscribeMsg);
    addReplyBulk(c,channel);
    addReplyLongLong(c,type.subscriptionCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub unsubscription notification to the client.
 * Channel can be NULL: this is useful when the client sends a mass
 * unsubscribe command but there are no channels to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubUnsubscribed(client *c, robj *channel, pubsubtype type) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c, *type.unsubscribeMsg);
    if (channel)
        addReplyBulk(c,channel);
    else
        addReplyNull(c);
    addReplyLongLong(c,type.subscriptionCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub pattern subscription notification to the client. */
void addReplyPubsubPatSubscribed(client *c, robj *pattern) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.psubscribebulk);
    addReplyBulk(c,pattern);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Send the pubsub pattern unsubscription notification to the client.
 * Pattern can be NULL: this is useful when the client sends a mass
 * punsubscribe command but there are no pattern to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubPatUnsubscribed(client *c, robj *pattern) {
    uint64_t old_flags = c->flags;
    c->flags |= CLIENT_PUSHING;
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.punsubscribebulk);
    if (pattern)
        addReplyBulk(c,pattern);
    else
        addReplyNull(c);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    if (!(old_flags & CLIENT_PUSHING)) c->flags &= ~CLIENT_PUSHING;
}

/* Check if a pattern is a prefix pattern (e.g., "foo*").
 * Returns 1 if it's a prefix pattern, 0 otherwise. */
int isPrefixPattern(sds pattern) {
    size_t len = sdslen(pattern);
    if (len < 1) return 0;

    /* Check if the last character is '*' */
    if (pattern[len - 1] != '*') return 0;

    /* Check if there's only one '*' and it's at the end */
    for (size_t i = 0; i < len - 1; i++) {
        if (pattern[i] == '*' || pattern[i] == '?' || pattern[i] == '[' || pattern[i] == ']') return 0;
    }

    return 1;
}

/*-----------------------------------------------------------------------------
 * Pubsub low level API
 *----------------------------------------------------------------------------*/

/* Return the number of pubsub channels + patterns is handled. */
int serverPubsubSubscriptionCount(void) {
    return kvstoreSize(server.pubsub_channels) + dictSize(server.pubsub_patterns) + trie_size(server.pubsub_patterns_trie);
}

/* Return the number of pubsub shard level channels is handled. */
int serverPubsubShardSubscriptionCount(void) {
    return kvstoreSize(server.pubsubshard_channels);
}

/* Return the number of channels + patterns a client is subscribed to. */
int clientSubscriptionsCount(client *c) {
    return dictSize(c->pubsub_channels) + dictSize(c->pubsub_patterns);
}

/* Return the number of shard level channels a client is subscribed to. */
int clientShardSubscriptionsCount(client *c) {
    return dictSize(c->pubsubshard_channels);
}

dict* getClientPubSubChannels(client *c) {
    return c->pubsub_channels;
}

dict* getClientPubSubShardChannels(client *c) {
    return c->pubsubshard_channels;
}

/* Return the number of pubsub + pubsub shard level channels
 * a client is subscribed to. */
int clientTotalPubSubSubscriptionCount(client *c) {
    return clientSubscriptionsCount(c) + clientShardSubscriptionsCount(c);
}

void markClientAsPubSub(client *c) {
    if (!(c->flags & CLIENT_PUBSUB)) {
        c->flags |= CLIENT_PUBSUB;
        server.pubsub_clients++;
    }
}

void unmarkClientAsPubSub(client *c) {
    if (c->flags & CLIENT_PUBSUB) {
        c->flags &= ~CLIENT_PUBSUB;
        server.pubsub_clients--;
    }
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
int pubsubSubscribeChannel(client *c, robj *channel, pubsubtype type) {
    dictEntry *de, *existing;
    dict *clients = NULL;
    int retval = 0;
    unsigned int slot = 0;

    /* Add the channel to the client -> channels hash table */
    void *position = dictFindPositionForInsert(type.clientPubSubChannels(c),channel,NULL);
    if (position) { /* Not yet subscribed to this channel */
        retval = 1;
        /* Add the client to the channel -> list of clients hash table */
        if (server.cluster_enabled && type.shard) {
            slot = getKeySlot(channel->ptr);
        }

        de = kvstoreDictAddRaw(*type.serverPubSubChannels, slot, channel, &existing);

        if (existing) {
            clients = dictGetVal(existing);
            channel = dictGetKey(existing);
        } else {
            clients = dictCreate(&clientDictType);
            kvstoreDictSetVal(*type.serverPubSubChannels, slot, de, clients);
            incrRefCount(channel);
        }

        serverAssert(dictAdd(clients, c, NULL) != DICT_ERR);
        serverAssert(dictInsertAtPosition(type.clientPubSubChannels(c), channel, position));
        incrRefCount(channel);
    }
    /* Notify the client */
    addReplyPubsubSubscribed(c,channel,type);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify, pubsubtype type) {
    dictEntry *de;
    dict *clients;
    int retval = 0;
    int slot = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    if (dictDelete(type.clientPubSubChannels(c),channel) == DICT_OK) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        if (server.cluster_enabled && type.shard) {
            slot = getKeySlot(channel->ptr);
        }
        de = kvstoreDictFind(*type.serverPubSubChannels, slot, channel);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        serverAssertWithInfo(c, NULL, dictDelete(clients, c) == DICT_OK);
        if (dictSize(clients) == 0) {
            /* Free the dict and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            kvstoreDictDelete(*type.serverPubSubChannels, slot, channel);
        }
    }
    /* Notify the client */
    if (notify) {
        addReplyPubsubUnsubscribed(c,channel,type);
    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Unsubscribe all shard channels in a slot. */
void pubsubShardUnsubscribeAllChannelsInSlot(unsigned int slot) {
    if (!kvstoreDictSize(server.pubsubshard_channels, slot))
        return;

    kvstoreDictIterator *kvs_di = kvstoreGetDictSafeIterator(server.pubsubshard_channels, slot);
    dictEntry *de;
    while ((de = kvstoreDictIteratorNext(kvs_di)) != NULL) {
        robj *channel = dictGetKey(de);
        dict *clients = dictGetVal(de);
        /* For each client subscribed to the channel, unsubscribe it. */
        dictIterator *iter = dictGetIterator(clients);
        dictEntry *entry;
        while ((entry = dictNext(iter)) != NULL) {
            client *c = dictGetKey(entry);
            int retval = dictDelete(c->pubsubshard_channels, channel);
            serverAssertWithInfo(c,channel,retval == DICT_OK);
            addReplyPubsubUnsubscribed(c, channel, pubSubShardType);
            /* If the client has no other pubsub subscription,
             * move out of pubsub mode. */
            if (clientTotalPubSubSubscriptionCount(c) == 0) {
                unmarkClientAsPubSub(c);
            }
        }
        dictReleaseIterator(iter);
        kvstoreDictDelete(server.pubsubshard_channels, slot, channel);
    }
    kvstoreReleaseDictIterator(kvs_di);
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. */
int pubsubSubscribePattern(client *c, robj *pattern) {
    dict *clients;
    int retval = 0;

    if (dictAdd(c->pubsub_patterns, pattern, NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(pattern);
        /* Add the client to the pattern -> list of clients hash table */
        sds prefix_sds = sdsdup(pattern->ptr);
        int is_prefix = isPrefixPattern(pattern->ptr);
        if (is_prefix) {
            sdssubstr(prefix_sds, 0, sdslen(prefix_sds)-1);
            clients = trie_lookup(server.pubsub_patterns_trie, prefix_sds);
        } else {
            dictEntry *de = dictFind(server.pubsub_patterns,pattern);
            if (de == NULL) {
                clients = NULL;
            } else {
                clients = dictGetVal(de);
            }
        }
        if (clients == NULL) {
            clients = dictCreate(&clientDictType);
            if (is_prefix) {
                trie_insert(server.pubsub_patterns_trie, prefix_sds, clients);
            } else {
                dictAdd(server.pubsub_patterns,pattern,clients);
                incrRefCount(pattern);
            }
        }
        sdsfree(prefix_sds);
        serverAssert(dictAdd(clients, c, NULL) != DICT_ERR);
    }
    /* Notify the client */
    addReplyPubsubPatSubscribed(c,pattern);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify) {
    dict *clients;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    if (dictDelete(c->pubsub_patterns, pattern) == DICT_OK) {
        retval = 1;
        /* Remove the client from the pattern -> clients list hash table */
        sds prefix_sds = sdsdup(pattern->ptr);
        int is_prefix = isPrefixPattern(pattern->ptr);
        if (is_prefix) {
            sdssubstr(prefix_sds, 0, sdslen(prefix_sds)-1);
            clients = trie_lookup(server.pubsub_patterns_trie, prefix_sds);
            serverAssertWithInfo(c,NULL,clients != NULL);
        } else {
            dictEntry *de = dictFind(server.pubsub_patterns,pattern);
            serverAssertWithInfo(c,NULL,de != NULL);
            clients = dictGetVal(de);
        }
        serverAssertWithInfo(c, NULL, dictDelete(clients, c) == DICT_OK);
        if (dictSize(clients) == 0) {
            /* Free the dict and associated hash entry at all if this was
                * the latest client. */
            if (is_prefix) {
                trie_delete(server.pubsub_patterns_trie, prefix_sds);
                dictRelease(clients);
            } else {
                dictDelete(server.pubsub_patterns,pattern);
            }
        }
        sdsfree(prefix_sds);
    }
    /* Notify the client */
    if (notify) addReplyPubsubPatUnsubscribed(c,pattern);
    decrRefCount(pattern);
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
int pubsubUnsubscribeAllChannelsInternal(client *c, int notify, pubsubtype type) {
    int count = 0;
    if (dictSize(type.clientPubSubChannels(c)) > 0) {
        dictIterator *di = dictGetSafeIterator(type.clientPubSubChannels(c));
        dictEntry *de;

        while((de = dictNext(di)) != NULL) {
            robj *channel = dictGetKey(de);

            count += pubsubUnsubscribeChannel(c,channel,notify,type);
        }
        dictReleaseIterator(di);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReplyPubsubUnsubscribed(c,NULL,type);
    }
    return count;
}

/*
 * Unsubscribe a client from all global channels.
 */
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c,notify,pubSubType);
    return count;
}

/*
 * Unsubscribe a client from all shard subscribed channels.
 */
int pubsubUnsubscribeShardAllChannels(client *c, int notify) {
    int count = pubsubUnsubscribeAllChannelsInternal(c, notify, pubSubShardType);
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
int pubsubUnsubscribeAllPatterns(client *c, int notify) {
    int count = 0;

    if (dictSize(c->pubsub_patterns) > 0) {
        dictIterator *di = dictGetSafeIterator(c->pubsub_patterns);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            robj *pattern = dictGetKey(de);
            count += pubsubUnsubscribePattern(c, pattern, notify);
        }
        dictReleaseIterator(di);
    }

    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) addReplyPubsubPatUnsubscribed(c,NULL);
    return count;
}

/*
 * Publish a message to all the subscribers.
 */
int pubsubPublishMessageInternal(robj *channel, robj *message, pubsubtype type) {
    int receivers = 0;
    dictEntry *de;
    dictIterator *di;
    unsigned int slot = 0;

    /* Send to clients listening for that channel */
    if (server.cluster_enabled && type.shard) {
        slot = keyHashSlot(channel->ptr, sdslen(channel->ptr));
    }
    de = kvstoreDictFind(*type.serverPubSubChannels, slot, channel);
    if (de) {
        dict *clients = dictGetVal(de);
        dictEntry *entry;
        dictIterator *iter = dictGetIterator(clients);
        while ((entry = dictNext(iter)) != NULL) {
            client *c = dictGetKey(entry);
            addReplyPubsubMessage(c,channel,message,*type.messageBulk);
            updateClientMemUsageAndBucket(c);
            receivers++;
        }
        dictReleaseIterator(iter);
    }

    if (type.shard) {
        /* Shard pubsub ignores patterns. */
        return receivers;
    }

    /* Send to clients listening to matching channels */
    channel = getDecodedObject(channel);
    /* Match trie first */
    trie_iterator *it = trie_iterator_new(server.pubsub_patterns_trie, channel->ptr);
    if (it) {
        trie_iterator_set_ends_with_star(it, 1);
        sds iter_key = NULL;
        dict *iter_clients;
        while ((iter_clients = trie_iterator_next(it, &iter_key)) != NULL) {
            robj *pattern = createStringObject(iter_key, sdslen(iter_key));
            dictEntry *entry;
            dictIterator *iter = dictGetIterator(iter_clients);
            while ((entry = dictNext(iter)) != NULL) {
                client *c = dictGetKey(entry);
                addReplyPubsubPatMessage(c, pattern, channel, message);
                updateClientMemUsageAndBucket(c);
                receivers++;
            }
            dictReleaseIterator(iter);
            decrRefCount(pattern);
            sdsfree(iter_key);
        }
        trie_iterator_free(it);
    }
    /* Match dict */
    di = dictGetIterator(server.pubsub_patterns);
    if (di) {
        while((de = dictNext(di)) != NULL) {
            robj *pattern = dictGetKey(de);
            dict *clients = dictGetVal(de);
            if (!stringmatchlen((char*)pattern->ptr,
                                sdslen(pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) continue;

            dictEntry *entry;
            dictIterator *iter = dictGetIterator(clients);
            while ((entry = dictNext(iter)) != NULL) {
                client *c = dictGetKey(entry);
                addReplyPubsubPatMessage(c,pattern,channel,message);
                updateClientMemUsageAndBucket(c);
                receivers++;
            }
            dictReleaseIterator(iter);
        }
        dictReleaseIterator(di);
    }
    decrRefCount(channel);
    return receivers;
}

/* Publish a message to all the subscribers. */
int pubsubPublishMessage(robj *channel, robj *message, int sharded) {
    return pubsubPublishMessageInternal(channel, message, sharded? pubSubShardType : pubSubType);
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

/* SUBSCRIBE channel [channel ...] */
void subscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backward compatibility
         */
        addReplyError(c, "SUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }
    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j],pubSubType);
    markClientAsPubSub(c);
}

/* UNSUBSCRIBE [channel ...] */
void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1,pubSubType);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* PSUBSCRIBE pattern [pattern ...] */
void psubscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backward compatibility
         */
        addReplyError(c, "PSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    markClientAsPubSub(c);
}

/* PUNSUBSCRIBE [pattern [pattern ...]] */
void punsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

/* This function wraps pubsubPublishMessage and also propagates the message to cluster.
 * Used by the commands PUBLISH/SPUBLISH and their respective module APIs.*/
int pubsubPublishMessageAndPropagateToCluster(robj *channel, robj *message, int sharded) {
    int receivers = pubsubPublishMessage(channel, message, sharded);
    if (server.cluster_enabled)
        clusterPropagatePublish(channel, message, sharded);
    return receivers;
}

/* PUBLISH <channel> <message> */
void publishCommand(client *c) {
    if (server.sentinel_mode) {
        sentinelPublishCommand(c);
        return;
    }

    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],0);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* PUBSUB command for Pub/Sub introspection. */
void pubsubCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CHANNELS [<pattern>]",
"    Return the currently active channels matching a <pattern> (default: '*').",
"NUMPAT",
"    Return number of subscriptions to patterns.",
"NUMSUB [<channel> ...]",
"    Return the number of subscribers for the specified channels, excluding",
"    pattern subscriptions(default: no channels).",
"SHARDCHANNELS [<pattern>]",
"    Return the currently active shard level channels matching a <pattern> (default: '*').",
"SHARDNUMSUB [<shardchannel> ...]",
"    Return the number of subscribers for the specified shard level channel(s)",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"channels") &&
        (c->argc == 2 || c->argc == 3))
    {
        /* PUBSUB CHANNELS [<pattern>] */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        channelList(c, pat, server.pubsub_channels);
    } else if (!strcasecmp(c->argv[1]->ptr,"numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N] */
        int j;

        addReplyArrayLen(c,(c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            dict *d = kvstoreDictFetchValue(server.pubsub_channels, 0, c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c, d ? dictSize(d) : 0);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT */
        addReplyLongLong(c,dictSize(server.pubsub_patterns)+trie_size(server.pubsub_patterns_trie));
    } else if (!strcasecmp(c->argv[1]->ptr,"shardchannels") &&
        (c->argc == 2 || c->argc == 3)) 
    {
        /* PUBSUB SHARDCHANNELS */
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        channelList(c,pat,server.pubsubshard_channels);
    } else if (!strcasecmp(c->argv[1]->ptr,"shardnumsub") && c->argc >= 2) {
        /* PUBSUB SHARDNUMSUB [ShardChannel_1 ... ShardChannel_N] */
        int j;
        addReplyArrayLen(c, (c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            unsigned int slot = calculateKeySlot(c->argv[j]->ptr);
            dict *clients = kvstoreDictFetchValue(server.pubsubshard_channels, slot, c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            addReplyLongLong(c, clients ? dictSize(clients) : 0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

void channelList(client *c, sds pat, kvstore *pubsub_channels) {
    long mblen = 0;
    void *replylen;
    unsigned int slot_cnt = kvstoreNumDicts(pubsub_channels);

    replylen = addReplyDeferredLen(c);
    for (unsigned int i = 0; i < slot_cnt; i++) {
        if (!kvstoreDictSize(pubsub_channels, i))
            continue;
        kvstoreDictIterator *kvs_di = kvstoreGetDictIterator(pubsub_channels, i);
        dictEntry *de;
        while((de = kvstoreDictIteratorNext(kvs_di)) != NULL) {
            robj *cobj = dictGetKey(de);
            sds channel = cobj->ptr;

            if (!pat || stringmatchlen(pat, sdslen(pat),
                                    channel, sdslen(channel),0))
            {
                addReplyBulk(c,cobj);
                mblen++;
            }
        }
        kvstoreReleaseDictIterator(kvs_di);
    }
    setDeferredArrayLen(c,replylen,mblen);
}

/* SPUBLISH <shardchannel> <message> */
void spublishCommand(client *c) {
    int receivers = pubsubPublishMessageAndPropagateToCluster(c->argv[1],c->argv[2],1);
    if (!server.cluster_enabled)
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* SSUBSCRIBE shardchannel [shardchannel ...] */
void ssubscribeCommand(client *c) {
    if (c->flags & CLIENT_DENY_BLOCKING) {
        /* A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe. */
        addReplyError(c, "SSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (int j = 1; j < c->argc; j++) {
        pubsubSubscribeChannel(c, c->argv[j], pubSubShardType);
    }
    markClientAsPubSub(c);
}

/* SUNSUBSCRIBE [shardchannel [shardchannel ...]] */
void sunsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeShardAllChannels(c, 1);
    } else {
        for (int j = 1; j < c->argc; j++) {
            pubsubUnsubscribeChannel(c, c->argv[j], 1, pubSubShardType);
        }
    }
    if (clientTotalPubSubSubscriptionCount(c) == 0) {
        unmarkClientAsPubSub(c);
    }
}

size_t pubsubMemOverhead(client *c) {
    /* PubSub patterns */
    size_t mem = dictMemUsage(c->pubsub_patterns);
    /* Global PubSub channels */
    mem += dictMemUsage(c->pubsub_channels);
    /* Sharded PubSub channels */
    mem += dictMemUsage(c->pubsubshard_channels);
    return mem;
}

int pubsubTotalSubscriptions(void) {
    return dictSize(server.pubsub_patterns) +
           trie_size(server.pubsub_patterns_trie) +
           kvstoreSize(server.pubsub_channels) +
           kvstoreSize(server.pubsubshard_channels);
}
