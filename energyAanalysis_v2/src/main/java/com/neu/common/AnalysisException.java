package com.neu.common;
/**
 * 
 * @author taosh
 * 将异常装换为自定义异常，
 * 并跑向最上层，最上层捕获
 * 后续可以定义状态码，方便从日志中定位问题
 */
public class AnalysisException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1583575204069095798L;
	private String retCd ;  //异常对应的返回码
    private String msgDes;  //异常对应的描述信息
     
    public AnalysisException() {
        super();
    }
 
    public AnalysisException(String message) {
        super(message);
        msgDes = message;
    }
    
    public AnalysisException(String message, Throwable cause) {
        super(message, cause);
        msgDes = message;
    }
 
    public AnalysisException(String retCd, String msgDes) {
        super();
        this.retCd = retCd;
        this.msgDes = msgDes;
    }
 
    public String getRetCd() {
        return retCd;
    }
 
    public String getMsgDes() {
        return msgDes;
    }

}
