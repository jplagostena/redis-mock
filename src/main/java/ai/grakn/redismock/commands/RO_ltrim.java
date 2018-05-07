package ai.grakn.redismock.commands;

import ai.grakn.redismock.RedisBase;
import ai.grakn.redismock.Response;
import ai.grakn.redismock.Slice;
import ai.grakn.redismock.Utils;

import java.util.LinkedList;
import java.util.List;

import static ai.grakn.redismock.Utils.convertToInteger;
import static ai.grakn.redismock.Utils.deserializeObject;
import static ai.grakn.redismock.Utils.serializeObject;

public class RO_ltrim extends AbstractRedisOperation {

    RO_ltrim(RedisBase base, List<Slice> params) {
        super(base, params,3, null, null);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        int startIndex = convertToInteger(new String(params().get(1).data()));
        int endIndex = convertToInteger(new String(params().get(2).data()));

        Slice data = base().rawGet(key);

        if(data == null){
            return Response.OK;
        }

        LinkedList<Slice> list = deserializeObject(data);

        //if startIndex is out of bounds, it should remove the key
        try {
            startIndex = Utils.convertToPositiveIndexIfNeeded(startIndex, list.size());
        } catch (IndexOutOfBoundsException ioobe) {
            base().del(key);
            return Response.OK;
        }

        //if endIndex is greater than size, endIndex should be treated as lastElement
        try {
            endIndex = Utils.convertToPositiveIndexIfNeeded(endIndex, list.size());
        } catch (IndexOutOfBoundsException ioobe) {
            endIndex = list.size() - 1;
        }

        //it should remove the key
        if (startIndex > endIndex) {
            base().del(key);
            return Response.OK;
        }

        List<Slice> newList = list.subList(startIndex, endIndex + 1);

        base().rawPut(key, serializeObject(new LinkedList(newList)), -1L);
        return Response.OK;
    }
}
