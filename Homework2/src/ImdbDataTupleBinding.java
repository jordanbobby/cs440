
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.*;

public class ImdbDataTupleBinding extends TupleBinding<Object>
{
    public Object entryToObject(TupleInput in)
    {
    	Long fileKey = in.readLong();
        String fileName = in.readString();
        int fileSize = in.readInt();
        String content = in.readString();
        
        ImdbData imdb = new ImdbData(fileKey, fileName, fileSize, content);
        return imdb;
    }
    public void objectToEntry(Object o, TupleOutput out)
    {
        ImdbData imdb = (ImdbData)o;
        out.writeLong(imdb.getFileKey());
        out.writeString(imdb.getFileName());
        out.writeInt(imdb.getFileSize());
        out.writeString(imdb.getContent());

    }
}

