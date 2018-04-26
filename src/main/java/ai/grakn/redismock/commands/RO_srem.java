package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;
import ai.grakn.redismock.exception.InternalException;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

import static ai.grakn.redismock.Utils.deserializeObject;
import static ai.grakn.redismock.Utils.serializeObject;

public class RO_srem extends AbstractRedisOperation {

    RO_srem(RedisBase base, List<Slice> params) {
        super(base, params,null, 1, null);
    }

    Slice response() {
        Slice key = params().get(0);
        Slice data = base().rawGet(key);
        Set<Slice> set;

        set = data != null ? deserializeObject(data) : Sets.newHashSet();

        int deletions = 0;
        for (int i = 1; i < params().size(); i++) {
            if (set.remove(params().get(i))) {
                deletions++;
            }
        }

        try {
            base().rawPut(key, serializeObject(set), -1L);
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
        return Response.integer(deletions);
    }

}
