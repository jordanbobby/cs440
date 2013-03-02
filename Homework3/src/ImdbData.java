
public class ImdbData implements Cloneable {
	//private long fileKey_;
	private String fileName_;
	private String invIndex_;
	
	public ImdbData()
	{}
	
	public ImdbData(
			//Long fileKey,
			String invIndex,
			String fileName){
		//fileKey_ = fileKey;
		fileName_ = fileName;
		invIndex_ = invIndex;
	}
	
	public String getFileName() {
		return fileName_;
	}
	public void setFileName(String fileName) {
		this.fileName_ = fileName;
	}
	
	public String getInvIndex() {
		return invIndex_;
	}
	public void setInvIndex(String invIndex) {
		this.invIndex_ = invIndex;
	}
	
	public String toString() {
		//return "ImdbData [fileKey=" + fileKey_ + ", fileName=" + fileName_ + ", fileSize=" + fileSize_ + ", content=" + content_ + "]";
		return "ImdbData [fileName=" + fileName_ + ", invIndex=" + invIndex_ + "]";
		
	}
}
