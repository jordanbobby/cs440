
public class ImdbData implements Cloneable {
	//private long fileKey_;
	private String fileName_;
	private int fileSize_;
	private String content_;
	
	public ImdbData()
	{}
	
	public ImdbData(
			//Long fileKey,
			String fileName,
			int fileSize,
			String content){
		//fileKey_ = fileKey;
		fileName_ = fileName;
		fileSize_ = fileSize;
		content_ = content;
	}
	/*
	public Long getFileKey(){
		return fileKey_;
	}
	public void setFileKey(Long fileKey){
		this.fileKey_ = fileKey;
	}
	*/
	public String getFileName() {
		return fileName_;
	}
	public void setFileName(String fileName) {
		this.fileName_ = fileName;
	}
	
	public int getFileSize() {
		return fileSize_;
	}
	public void setFileSize(int fileSize) {
		this.fileSize_ = fileSize;
	}
	
	public String getContent() {
		return content_;
	}
	public void setContent(String content) {
		this.content_ = content;
	}

	public String toString() {
		//return "ImdbData [fileKey=" + fileKey_ + ", fileName=" + fileName_ + ", fileSize=" + fileSize_ + ", content=" + content_ + "]";
		return "ImdbData [fileName=" + fileName_ + ", fileSize=" + fileSize_ + ", content=" + content_ + "]";
		
	}
}
