package com.nimbus.net.results;

import com.nimbus.proto.messages.ResponseMessage;

public abstract class QueryResult {

    final ResponseMessage responseMessage;
    final String host;

    public QueryResult(ResponseMessage responseMessage, String host) {
        this.responseMessage = responseMessage;
        this.host = host;
    }

    ResponseMessage getResponseMessage() {
        return responseMessage;
    }

    public int responseBytes() {
        return responseMessage.totalLength();
    }

    public int compression() {
        return responseMessage.compression();
    }

    public int count() {
        return responseMessage.count();
    }

    public void release() {
        responseMessage.release();
    }

}
