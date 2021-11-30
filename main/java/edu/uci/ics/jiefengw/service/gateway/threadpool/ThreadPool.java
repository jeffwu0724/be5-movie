package edu.uci.ics.jiefengw.service.gateway.threadpool;

import edu.uci.ics.jiefengw.service.gateway.logger.ServiceLogger;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ThreadPool
{
    private int numWorkers;

    private ArrayList<Worker> workers;
    private BlockingQueue<ClientRequest> queue;

    /*
     * BlockingQueue is a interface that allows us
     * to choose the type of implementation of the queue.
     * In this case we are using a LinkedBlockingQueue.
     *
     * BlockingQueue as the name implies will block
     * any thread requesting from it if the queue is empty
     * but only if you use the correct function
     */
    private ThreadPool(int numWorkers)
    {
        this.numWorkers = numWorkers;

        workers = new ArrayList<>();
        queue = new LinkedBlockingQueue<>();

        // TODO more work is needed to create the threads
        for(int i = 0; i < numWorkers; i++){
            Worker worker = Worker.CreateWorker(i,this);
            workers.add(worker);
        }
        //start all the worker threads.
        for(Worker worker: workers){
            worker.start();
            //if we use .run() here, it will wait until it finish
        }
    }

    public static ThreadPool createThreadPool(int numWorkers)
    {
        return new ThreadPool(numWorkers);
    }

    /*
     * Note that this function only has package scoped
     * as it should only be called with the package by
     * a worker
     * 
     * Make sure to use the correct functions that will
     * block a thread if the queue is unavailable or empty
     */
    ClientRequest takeRequest(){
        // TODO *take* the request from the queue
        //return null;
       try{
           return queue.take();
       } catch (InterruptedException e) {
           e.printStackTrace();
           //ServiceLogger.LOGGER("Error in take request");
       }
       return null;
    }

    public void putRequest(ClientRequest req){
        // TODO *put* the request into the queue
        try{
            queue.put(req);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

}
