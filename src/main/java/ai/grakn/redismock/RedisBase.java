package ai.grakn.redismock;

import ai.grakn.redismock.commands.RedisType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {
    private final Map<Slice, Set<RedisClient>> subscribers = new ConcurrentHashMap<>();
    private final Map<Slice, Slice> base = new ConcurrentHashMap<>();
    private final Map<Slice, Long> deadlines = new ConcurrentHashMap<>();
    private final Set<RedisBase> syncBases = ConcurrentHashMap.newKeySet();
    private final Map<RedisType, Set<Slice>> elementsByType = new ConcurrentHashMap<>();

    public RedisBase() {}

    public void addSyncBase(RedisBase base) {
        syncBases.add(base);
    }

    public Set<Slice> keys(){
        return base.keySet();
    }

    public Slice rawGet(Slice key) {
        Preconditions.checkNotNull(key);

        Long deadline = deadlines.get(key);
        if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
            this.removeElement(key);
            return null;
        }
        return base.get(key);
    }

    public Long getTTL(Slice key) {
        Preconditions.checkNotNull(key);

        Long deadline = deadlines.get(key);
        if (deadline == null) {
            return null;
        }
        if (deadline == -1) {
            return deadline;
        }
        long now = System.currentTimeMillis();
        if (now < deadline) {
            return deadline - now;
        }
        this.removeElement(key);
        return null;
    }

    public long setTTL(Slice key, long ttl) {
        Preconditions.checkNotNull(key);

        if (base.containsKey(key)) {
            deadlines.put(key, ttl + System.currentTimeMillis());
            for (RedisBase base : syncBases) {
                base.setTTL(key, ttl);
            }
            return 1L;
        }
        return 0L;
    }

    public long setDeadline(Slice key, long deadline) {
        Preconditions.checkNotNull(key);

        if (base.containsKey(key)) {
            deadlines.put(key, deadline);
            for (RedisBase base : syncBases) {
                base.setDeadline(key, deadline);
            }
            return 1L;
        }
        return 0L;
    }

    public void clear(){
        base.clear();
        subscribers.clear();
        deadlines.clear();
        syncBases.clear();
        elementsByType.clear();
    }

    /**
     * Tracks the element before the raw put.
     * It is useful to track which type is each element
     *
     * @param key key to store
     * @param value value to store in the base
     * @param ttl time to live
     * @param type the {@link RedisType} of the value to be stored
     */
    public void rawPut(Slice key, Slice value, Long ttl, RedisType type) {
        this.trackElement(key, type);
        this.rawPut(key, value, ttl);
    }

    public void rawPut(Slice key, Slice value, Long ttl) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);


        base.put(key, value);

        if (ttl != null) {
            if (ttl != -1) {
                deadlines.put(key, ttl + System.currentTimeMillis());
            } else {
                deadlines.put(key, -1L);
            }
        }
        for (RedisBase base : syncBases) {
            base.rawPut(key, value, ttl);
        }
    }

    public void del(Slice key) {
        Preconditions.checkNotNull(key);

        this.removeElement(key);

        for (RedisBase base : syncBases) {
            base.del(key);
        }
    }

    private void removeElement(Slice key) {
        this.unTrackElement(key);
        base.remove(key);
        deadlines.remove(key);
    }

    public void addSubscriber(Slice channel, RedisClient client){
        Set<RedisClient> newClient = new HashSet<>();
        newClient.add(client);
        subscribers.merge(channel, newClient, (currentSubscribers, newSubscribers) -> {
            currentSubscribers.addAll(newSubscribers);
            return currentSubscribers;
        });
    }

    public boolean removeSubscriber(Slice channel, RedisClient client){
        if(subscribers.containsKey(channel)){
            subscribers.get(channel).remove(client);
            return true;
        }
        return false;
    }

    public Set<RedisClient> getSubscribers(Slice channel){
        if (subscribers.containsKey(channel)) {
            return subscribers.get(channel);
        }
        return Collections.emptySet();
    }

    public List<Slice> getSubscriptions(RedisClient client){
        List<Slice> subscriptions = new ArrayList<>();

        subscribers.forEach((channel, subscribers) -> {
            if(subscribers.contains(client)){
                subscriptions.add(channel);
            }
        });

        return subscriptions;
    }

    /**
     * Tracks the creation of an element
     *
     * @param elementName key of the element
     * @param type one of the {@link RedisType}
     */
    private void trackElement(Slice elementName, RedisType type) {
        if (elementsByType.isEmpty()) {
            this.initializeElementTypeTracking();
        }
        Set<Slice> elementSet = elementsByType.get(type);
        elementSet.add(elementName);
    }

    private void unTrackElement(Slice elementName) {
        if (elementsByType.isEmpty()) {
            this.initializeElementTypeTracking();
        }
        RedisType redisType = this.getElementType(elementName);
        Set<Slice> elementSet = elementsByType.get(redisType);
        if (elementSet != null) {
            elementSet.remove(elementName);
        }
    }

    public RedisType getElementType(Slice elementName) {
        Set<Map.Entry<RedisType, Set<Slice>>> entries = elementsByType.entrySet();
        for (Map.Entry<RedisType, Set<Slice>> entry: entries) {
            if (entry.getValue().contains(elementName)) {
                return entry.getKey();
            }
        }
        return RedisType.NONE;
    }

    private void initializeElementTypeTracking() {
        this.elementsByType.put(RedisType.HASH, Sets.newHashSet());
        this.elementsByType.put(RedisType.LIST, Sets.newHashSet());
        this.elementsByType.put(RedisType.SET, Sets.newHashSet());
        this.elementsByType.put(RedisType.ZSET, Sets.newHashSet());
        this.elementsByType.put(RedisType.STRING, Sets.newHashSet());
    }
}
