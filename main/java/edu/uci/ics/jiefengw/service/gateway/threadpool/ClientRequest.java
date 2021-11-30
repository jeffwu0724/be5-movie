package edu.uci.ics.jiefengw.service.gateway.threadpool;

import javax.ws.rs.core.HttpHeaders;

public class ClientRequest
{

    /* User Information */
    public String email;
    public String session_id;
    public String transaction_id;
    public HttpHeaders header;

    /* Target Service and Endpoint */
    public String URI;
    public String endpoint;
    public HTTPMethod method;

    /*
     * So before when we wanted to get the request body
     * we would grab it as a String (String jsonText).
     *
     * The Gateway however does not need to see the body
     * but simply needs to pass it. So we save ourselves some
     * time and overhead by grabbing the request as a byte array
     * (byte[] jsonBytes).
     *
     * This way we can just act as a
     * messenger and just pass along the bytes to the target
     * service and it will do the rest.
     *
     * for example:
     *
     * where we used to do this:
     *
     *     @Path("hello")
     *     ...ect
     *     public Response hello(String jsonString) {
     *         ...ect
     *     }
     *
     * do:
     *
     *     @Path("hello")
     *     ...ect
     *     public Response hello(byte[] jsonBytes) {
     *         ...ect
     *     }
     *
     */
    public byte[] requestBytes;
    public ClientRequest() { }
    public ClientRequest(String email, String session_id, String transaction_id,byte[] requestBytes,
                         String URI, String endpoint, HTTPMethod method, HttpHeaders header)
    {
        this.email = email;
        this.session_id = session_id;
        this.transaction_id = transaction_id;
        this.requestBytes = requestBytes;
        this.URI = URI;
        this.endpoint = endpoint;
        this.method = method;
        this.header = header;
    }

    public ClientRequest(HTTPMethod method, String uri, String endpoint, HttpHeaders headers, byte[] args, String transaction_id){
        this.method = method;
        this.URI = uri;
        this.endpoint = endpoint;
        this.header = headers;
        this.requestBytes = args;
        this.transaction_id = transaction_id;
    }
}
