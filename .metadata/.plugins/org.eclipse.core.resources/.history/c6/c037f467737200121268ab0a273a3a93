import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;


public class ImdbDataTupleBinding extends TupleBinding<Object> {

	@Override
	public Object entryToObject(TupleInput in) {
		ImdbData imdb = new ImdbData();
		
		imdb.setFileName(in.readInt());
		imdb.setFileSize(in.readInt());
		imdb.setContent(in.readString());
		
		return imdb;
	}

	@Override
	public void objectToEntry(Object o, TupleOutput out) {
		// TODO Auto-generated method stub
		ImdbData imdb = (ImdbData)o;
		
		out.writeInt(imdb.getFileName());
		out.writeInt(imdb.getFileSize());
		out.writeString(imdb.getContent());
		
	}

}
