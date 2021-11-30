package edu.uci.ics.jiefengw.service.gateway.resources;

import edu.uci.ics.jiefengw.service.gateway.GatewayService;
import edu.uci.ics.jiefengw.service.gateway.configs.BillingConfigs;
import edu.uci.ics.jiefengw.service.gateway.configs.IdmConfigs;
import edu.uci.ics.jiefengw.service.gateway.configs.MoviesConfigs;
import edu.uci.ics.jiefengw.service.gateway.logger.ServiceLogger;
import edu.uci.ics.jiefengw.service.gateway.threadpool.ClientRequest;
import edu.uci.ics.jiefengw.service.gateway.threadpool.HTTPMethod;
import edu.uci.ics.jiefengw.service.gateway.threadpool.*;
import edu.uci.ics.jiefengw.service.gateway.transaction.TransactionGenerator;

import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Path("")
public class GatewayResource {


    //private ThreadPool threadPool;

    private UriBuilder getServiceURI(String service, String endpoint){
        UriBuilder builder = null;
        ServiceLogger.LOGGER.severe(service + " " + endpoint);
        if(service.equals("idm")){
            IdmConfigs configs = GatewayService.getIdmConfigs();
            builder = UriBuilder.fromUri(configs.getScheme() + configs.getHostName() + configs.getPath()).port(configs.getPort());
            switch(endpoint){
                case "login":
                    builder = builder.path(configs.getLoginPath());
                    break;
                case "register":
                    builder = builder.path(configs.getRegisterPath());
                    break;
                case "session":
                    builder = builder.path(configs.getSessionPath());
                    break;
                case "privilege":
                    builder = builder.path(configs.getPrivilegePath());
                    break;
                default:
                    return null;
            }
        }
        if(service.equals("movies")){
            MoviesConfigs configs = GatewayService.getMoviesConfigs();
            builder = UriBuilder.fromUri(configs.getScheme() + configs.getHostName() + configs.getPath()).port(configs.getPort());
            ServiceLogger.LOGGER.info("endpoint: " + endpoint);

            String temp = endpoint;
            ServiceLogger.LOGGER.info("temp: " + temp);
            String toAdd = "";
            if(endpoint.contains("/")){

                String[] output =  endpoint.split("/");
                toAdd = output[1];
                ServiceLogger.LOGGER.info("output[0]: " + output[0] + " output[1]: "+ output[1]);
                if(output[0].equals("get") ){
                    endpoint = "get/";
                }else if(output[0].equals("people") && output[1].equals("get")){
                    endpoint = "people/get/";
                }
                else if(output[0].equals("browse")){
                    endpoint = "browse/";
                }
            }

            switch(endpoint){
                case "search":
                    endpoint = temp;
                    builder = builder.path(configs.getSearchPath());
                    break;
                case "browse/":
                    endpoint = temp;
                    builder = builder.path(configs.getBrowsePath()+ toAdd);
                    break;
                case "get/":
                    endpoint = temp;
                    ServiceLogger.LOGGER.info("endpoint: " + endpoint);
                    builder = builder.path(configs.getGetPath() + toAdd);

                    break;
                case "thumbnail":
                    endpoint = temp;
                    builder = builder.path(configs.getThumbnailPath());
                    break;
                case "people":
                    endpoint = temp;
                    builder = builder.path(configs.getPeoplePath());
                    break;
                case "people/search":
                    endpoint = temp;
                    builder = builder.path(configs.getPeopleSearchPath());
                    break;
                case "people/get/":
                    endpoint = temp;
                    ServiceLogger.LOGGER.info("endpoint: " + endpoint);
                    builder = builder.path(configs.getPeopleGetPath());

                    break;
                default:
                    return null;
            }
        }
        if(service.equals("billing")){
            ServiceLogger.LOGGER.info("in billing...");
            BillingConfigs configs = GatewayService.getBillingConfigs();
            builder = UriBuilder.fromUri(configs.getScheme() + configs.getHostName() + configs.getPath()).port(configs.getPort());
            ServiceLogger.LOGGER.info("After build...");
            ServiceLogger.LOGGER.info("endpoint: " + endpoint);
            switch(endpoint){
                case "cart/insert":
                    builder = builder.path(configs.getCartInsertPath());
                    break;
                case "cart/update":
                    builder = builder.path(configs.getCartUpdatePath());
                    break;
                case "cart/delete":
                    builder = builder.path(configs.getCartDeletePath());
                    break;
                case "cart/retrieve":
                    builder = builder.path(configs.getCartRetrievePath());
                    break;
                case "cart/clear":
                    builder = builder.path(configs.getCartClearPath());
                    break;
                case "order/place":
                    builder = builder.path(configs.getOrderPlacePath());
                    break;
                case "order/retrieve":
                    builder = builder.path(configs.getOrderRetrievePath());
                    break;
                case "order/complete":
                    builder = builder.path(configs.getOrderCompletePath());
                    break;
                case "discount/create":
                    ServiceLogger.LOGGER.info("discount/create...");
                    builder = builder.path(configs.getDiscountCreatePath());
                    break;
                case "discount/apply":
                    ServiceLogger.LOGGER.info("discount/apply...");
                    builder = builder.path(configs.getDiscountApplyPath());
                    break;
                default:
                    ServiceLogger.LOGGER.info("builder is null in switch...");
                    return null;
            }
        }

        if(builder == null){
            ServiceLogger.LOGGER.info("builder is null...");
            return null;
        }
        return builder;
    }

    private Response do_request(HTTPMethod method, String uri, String endpoint, HttpHeaders headers, byte[] args){
        //TODO create request object
        //String email, String session_id, String transaction_id, String URI, String endpoint, HTTPMethod method, HttpHeaders header
        String trans_id = TransactionGenerator.generate();
        ServiceLogger.LOGGER.severe("trans_id: " + trans_id );
        ServiceLogger.LOGGER.severe("uri: " + uri );
        ClientRequest req = new ClientRequest(method, uri, endpoint, headers,args, trans_id);
        //TODO put request object into queue
        //this.threadPool.putRequest(req);
        GatewayService.getThreadPool().putRequest(req);
        //send no content reply
        return Response.status(Response.Status.NO_CONTENT)
                .header("transaction_id", req.transaction_id)
                .header("request_delay", 1)
                .build();

    }

    @POST
    @Path("{service}/{endpoint: .+}")
    public Response request(@Context HttpHeaders headers, @PathParam("service") String service, @PathParam("endpoint") String endpoint, byte[] args){
        ServiceLogger.LOGGER.severe("Post - {service}/{endpoint: .+}");
        String uri = getServiceURI(service, endpoint).build().toString();
        ServiceLogger.LOGGER.severe("uri: " + uri);
        if (uri == null){
            ServiceLogger.LOGGER.severe(String.format("Unknown service %s and endpoint %s",service, endpoint ));
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }

        return do_request(HTTPMethod.POST, uri, endpoint, headers, args);
    }

    @GET
    @Path("{service}/{endpoint: .+}")
    public Response request(@Context HttpHeaders headers, @Context UriInfo uriinfo,
                            @PathParam("service") String service, @PathParam("endpoint") String endpoint,
                            byte[] args){
        ServiceLogger.LOGGER.severe("GET - {service}/{endpoint: .+}");
        ServiceLogger.LOGGER.severe("service: " +service );
        ServiceLogger.LOGGER.severe("endpoint: " + endpoint );
        UriBuilder uribuilder = getServiceURI(service, endpoint);
        ServiceLogger.LOGGER.severe("uriibuilder: " + uribuilder);
        //String uri = getServiceURI(service, endpoint).build().toString();
        if (uribuilder == null){
            ServiceLogger.LOGGER.severe(String.format("Unknown service %s and endpoint %s",service, endpoint ));
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        MultivaluedMap<String, String> params = uriinfo.getQueryParameters();
        for(String k: params.keySet()){
           //
            // uribuilder = uribuilder.queryParam(k, params.get(k));
            uribuilder.replaceQuery(uriinfo.getRequestUri().getQuery());
        }
        String uri = uribuilder.build().toString();
        return do_request(HTTPMethod.GET, uri, endpoint, headers, null);
    }

    @GET
    @Path("report")
    public Response report(@Context HttpHeaders headers){
        ServiceLogger.LOGGER.severe("GET - report");
        String transaction_id = headers.getHeaderString("transaction_id");
        ServiceLogger.LOGGER.info("transaction_id: " + transaction_id);
        if(transaction_id == null){
            ServiceLogger.LOGGER.severe("No transaction ID supplied with report request.");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }

        Integer response_status = null;
        String response_string = null;

        //Find response in database
        ServiceLogger.LOGGER.info(String.format("Looking for transaction results for %s in database",transaction_id));
        Connection con = null;
        try{
            //Request connection from Hikari.
            con = GatewayService.getConnectionPoolManager().requestCon();

            //SELECT transaction by ID from database
            String base_query = "SELECT DISTINCT responses.* " + //, genre.name AS 'genre'
                    "FROM responses " +
                    "WHERE responses.transaction_id = ?"; // and cart.quantity = ?
            // Create the prepared statement
            PreparedStatement base_ps = con.prepareStatement(base_query);

            base_ps.setString(1, transaction_id);
            // base_ps.setInt(2, requestModel.getQuantity());

            ServiceLogger.LOGGER.info("Trying selection: " + base_ps.toString());
            ResultSet rs = base_ps.executeQuery();
            ServiceLogger.LOGGER.info("selection succeeded.");

            //Put result into response_status, response_string
            while (rs.next()) {
                response_status = rs.getInt("http_status");
                response_string = rs.getString("response");
            }
            ServiceLogger.LOGGER.info("response_status: " + response_status);
            ServiceLogger.LOGGER.info(" response_string: " +  response_string);

            //Delete from database
            // Construct the query
            String delete_query =  "DELETE FROM responses " +
                    "WHERE responses.transaction_id = ?";

            // Create the prepared statement
            PreparedStatement delete_ps = con.prepareStatement(delete_query);

            // Set the arguments
            delete_ps.setString(1, transaction_id);

            // Save the query result to a ResultSet so records may be retrieved
            ServiceLogger.LOGGER.info("Trying delete: " + delete_ps.toString());
            // code = ps_user_status.executeUpdate();
            // code = ps_privilege_level.executeUpdate();
            delete_ps.executeUpdate();
            ServiceLogger.LOGGER.info("delete succeeded.");

        }catch(SQLException e){
            ServiceLogger.LOGGER.severe("SQL ERROR");
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }finally{
           if(con != null){
               GatewayService.getConnectionPoolManager().releaseCon(con);
           }
        }

        if(response_status != null){
            //TODO also copy over the headers of the callback response
            return Response.status(response_status).entity(response_string).build();
        }
        return Response.status(Response.Status.NO_CONTENT)
                .header("message", "No result available yet.")
                .header("request_delay", 1)
                .header("transaction_id", transaction_id)
                .build();
    }
}
