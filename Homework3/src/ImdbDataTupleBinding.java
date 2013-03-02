
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.*;

public class ImdbDataTupleBinding extends TupleBinding<Object>
{
    public Object entryToObject(TupleInput in)
    {
    	//Long fileKey = in.readLong();
    	String invIndex = in.readString();
        String fileName = in.readString();
        //int fileSize = in.readInt();
        //String content = in.readString();
        
        
        //ImdbData imdb = new ImdbData(fileKey, fileName, fileSize, content);
        ImdbData imdb = new ImdbData(invIndex, fileName);
        
        return imdb;
    }
    public void objectToEntry(Object o, TupleOutput out)
    {
        ImdbData imdb = (ImdbData)o;
        out.writeString(imdb.getInvIndex());
        out.writeString(imdb.getFileName());
    }
}

