package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.logging.internal.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NeuPrintTransactionEventHandlerExtensionFactory extends KernelExtensionFactory<NeuPrintTransactionEventHandlerExtensionFactory.Dependencies> {

    @Override
    public Lifecycle newInstance(KernelContext kernelContext, final Dependencies dependencies) {
        return new LifecycleAdapter() {

            private NeuPrintTransactionEventHandler handler;
            private ExecutorService executor;
            private Log userLog;

            @Override
            public void start() {
                executor = Executors.newFixedThreadPool(1);
                userLog = dependencies.log().getUserLog(NeuPrintTransactionEventHandlerExtensionFactory.class);
                handler = new NeuPrintTransactionEventHandler(dependencies.getGraphDatabaseService(), executor, userLog);
                dependencies.getGraphDatabaseService().registerTransactionEventHandler(handler);
            }

            @Override
            public void shutdown() {
                executor.shutdown();
                dependencies.getGraphDatabaseService().unregisterTransactionEventHandler(handler);
            }
        };
    }



    interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
        LogService log();
    }

    public NeuPrintTransactionEventHandlerExtensionFactory() {
        super(ExtensionType.DATABASE, "registerTransactionEventHandler");
    }

}

