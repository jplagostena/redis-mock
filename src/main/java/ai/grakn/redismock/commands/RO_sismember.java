package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;

import java.util.List;
import java.util.Set;

import static ai.grakn.redismock.Utils.deserializeObject;

public class RO_sismember extends AbstractRedisOperation {


    RO_sismember(RedisBase base, List<Slice> params) {
        super(base, params,null, 1, null);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Slice data = base().rawGet(key);
        Set<Slice> set;

        //there is no set with key
        if (data == null) {
            return Response.integer(0);
        }

        set = deserializeObject(data);

        return set.contains(params().get(1)) ? Response.integer(1) : Response.integer(0) ;
    }
}
