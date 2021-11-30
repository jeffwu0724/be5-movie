package edu.uci.ics.jiefengw.service.gateway.threadpool;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.zaxxer.hikari.HikariConfig;
import edu.uci.ics.jiefengw.service.gateway.GatewayService;
import edu.uci.ics.jiefengw.service.gateway.connectionpool.ConnectionPoolManager;
import edu.uci.ics.jiefengw.service.gateway.logger.ServiceLogger;
import org.checkerframework.checker.units.qual.C;


import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Worker extends Thread {
    int id;
    ThreadPool threadPool;


    private Worker(int id, ThreadPool threadPool) {
        this.id = id;
        this.threadPool = threadPool;
    }

//    public static Worker CreateWorker(int id, ThreadPool threadPool) {
//        return null;
//    }
    public static Worker CreateWorker(int id, ThreadPool threadPool) {
        return new Worker(id, threadPool);
    }

    public void process(ClientRequest req) {
        //Process request by forwarding it to the appropriate endpoint,
        //storing the request in the database.

        //prepare request to send
        ServiceLogger.LOGGER.info("Building client...");
        Client client = ClientBuilder.newClient();
        ServiceLogger.LOGGER.info("Building WebTarget...");
        ServiceLogger.LOGGER.info(req.URI);
        WebTarget destination = client.target(req.URI);
        Invocation.Builder requester = destination.request(MediaType.APPLICATION_JSON);
        client.register(JacksonJsonProvider.class);


        //TODO Set request headers.
        requester.header("email",req.header.getHeaderString("email"));
        requester.header("session_id",req.header.getHeaderString("session_id"));
        requester.header("transaction_id",req.header.getHeaderString("transaction_id"));

        //TODO Set request entity.
        Entity entity = Entity.entity(req.requestBytes,MediaType.APPLICATION_JSON);
        //TODO send request.
        ServiceLogger.LOGGER.info("Sending request...");
        Response response; // = invocationBuilder.(Entity.entity(entity, MediaType.APPLICATION_JSON))
        ServiceLogger.LOGGER.info(String.format("Sending %s request to %s, endpoint %s.",
                req.method.toString(), req.URI, req.endpoint));
        ServiceLogger.LOGGER.info("Request sent.");

        //Response response;


        //Read response
        try{
            if(entity != null){
                response = requester.method(req.method.toString(), entity);
            }else{
                response = requester.method(req.method.toString());
            }
        }catch(ProcessingException ex){
            ServiceLogger.LOGGER.severe((ex.toString()));
            return;
        }

        int response_status = response.getStatus();
        String response_string = (response.hasEntity() ? response.readEntity(String.class) : "");
        ServiceLogger.LOGGER.info("response_string: " + response_string);

        //TODO need to change this service logger
        ServiceLogger.LOGGER.info(String.format("Request for transcation %s replied with status %s.", req.transaction_id, response_status));

        //insert response into table
        ServiceLogger.LOGGER.info(String.format("Inserting results of transaction %s into table.", req.transaction_id));
        Connection con = null;
        try{
            //Get connection from Hikari
            //ConnectionPoolManager createConPool(String url, String username, String password, int numCons)

            //HikariConfig config = ConnectionPoolManager.createConPool()
            //ConnectionPoolManager hikariConPool = new ConnectionPoolManager( );
            con = GatewayService.getConnectionPoolManager().requestCon();//GatewayService.getConnectionPoolManager().requestCon();

            //Store response into table
            // Construct the query
            String query =  "INSERT INTO responses (transaction_id, response, http_status)" +
                    " VALUE (?, ?, ?)";


            // Create the prepared statement
            PreparedStatement ps = con.prepareStatement(query);


            ServiceLogger.LOGGER.info("response_string: " + response_string);

            // Set the arguments
            ps.setString(1, req.transaction_id);
            ps.setString(2, response_string);
            ps.setInt(3, response_status);      //might change later

            // Save the query result to a ResultSet so records may be retrieved
            ServiceLogger.LOGGER.info("Trying insertion: " + ps.toString());
            // code = ps_user_status.executeUpdate();
            // code = ps_privilege_level.executeUpdate();
            ps.executeUpdate();
            ServiceLogger.LOGGER.info("Insertion succeeded.");
            // ServiceLogger.LOGGER.info(query);

            }
        catch(SQLException e){
            ServiceLogger.LOGGER.severe((String.format("Error processing transaction %s, cannot insert res " +
                    "table. SQL error: %s", req.transaction_id, e.toString())));
        }
        finally {
           GatewayService.getConnectionPoolManager().releaseCon(con);
        }
    }

    @Override
    public void run() {
        ServiceLogger.LOGGER.info(String.format("Worker thread %d started.", this.id));
        while (true) {
            //Get a request to process. If none available, block and wait.
            ClientRequest req = this.threadPool.takeRequest();
            ServiceLogger.LOGGER.info(String.format("Starting processing of request %s.", req.transaction_id));

            try{
                this.process(req);
            }catch(Exception e){
                ServiceLogger.LOGGER.severe(String.format("Uncaught exception occurred during processing"));
            }
        }
    }
}
