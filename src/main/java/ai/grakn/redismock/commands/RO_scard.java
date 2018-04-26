package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static ai.grakn.redismock.Utils.deserializeObject;

public class RO_scard extends AbstractRedisOperation {


    RO_scard(RedisBase base, List<Slice> params) {
        super(base, params, 1, null, null);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Slice data = base().rawGet(key);
        Set<Slice> set;

        set = data != null ? deserializeObject(data) : Sets.newHashSet();

        return Response.integer(set.size());

    }
}
