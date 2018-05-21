package com.dalong;

import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class Application {

    public static void main(String[] args) throws InterruptedException {
        String siddhiApp = "define stream StockEventStream (symbol string, price float, volume long); " +
                " " +
                "@info(name = 'query1') " +
                "from StockEventStream#window.time(5 sec)  " +
                "select symbol, sum(price) as price, sum(volume) as volume " +
                "group by symbol " +
                "insert into AggregateStockStream ;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
        siddhiAppRuntime.addCallback("AggregateStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] event) {
                for (int i = 0; i < event.length; i++) {
                    System.out.println("get data:"+event[i].toString());
                }
            }
        });
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockEventStream");
        siddhiAppRuntime.start();
        inputHandler.send(new Object[]{"IBM", 100f, 100L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 300L});
        inputHandler.send(new Object[]{"WSO2", 60f, 200L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 70f, 400L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        Thread.sleep(5000);
        inputHandler.send(new Object[]{"IBM", 200f, 400L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 70f, 50L});
        Thread.sleep(5000);
        inputHandler.send(new Object[]{"WSO2", 80f, 400L});
        inputHandler.send(new Object[]{"GOOG", 60f, 30L});
        Thread.sleep(3000);
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }
}

