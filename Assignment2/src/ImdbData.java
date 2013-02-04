
public class ImdbData implements Cloneable {
	
	private int fileName;
	private int fileSize;
	private String content;
	
	public ImdbData(){
		
	}
	
	public int getFileName() {
		return fileName;
	}
	public void setFileName(int fileName) {
		this.fileName = fileName;
	}
	public int getFileSize() {
		return fileSize;
	}
	public void setFileSize(int fileSize) {
		this.fileSize = fileSize;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "ImdbData [fileName=" + fileName + ", fileSize=" + fileSize
				+ ", content=" + content + "]";
	}
	

}
