package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;

import java.util.List;

public class RO_type extends AbstractRedisOperation {

    RO_type(RedisBase base, List<Slice> params) {
        super(base, params, 1, null, null);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        RedisType redisType = base().getElementType(key);

        return Response.bulkString(new Slice(redisType.getName()));
    }

}
