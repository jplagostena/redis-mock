package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static ai.grakn.redismock.Utils.deserializeObject;

class RO_sinter extends AbstractRedisOperation {

    RO_sinter(RedisBase base, List<Slice> params) {
        super(base, params,  null, null, null);
    }

    Slice response() {

        Set<Slice> set = base().rawGet(params().get(0)) == null ? Sets.newHashSet() :
                Sets.newHashSet(new LinkedList<>(deserializeObject(base().rawGet(params().get(0)))));
        Set<Slice> intersection = set;

        //for each param, we should get the intersection
        for (Slice slice : params()) {
            Slice data = base().rawGet(slice);
            set = data == null ? Sets.newHashSet() :
                    Sets.newHashSet(new LinkedList<>(deserializeObject(data)));
            intersection = Sets.newHashSet(Sets.intersection(intersection, set));
        }

        LinkedList<Slice> linkedListToResponse = Lists.newLinkedList(intersection);

        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<Slice>();
        linkedListToResponse.forEach(element -> builder.add(Response.bulkString(element)));

        return Response.array(builder.build());
    }
}
