/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cxf.transport.jms.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.transaction.Status;
import javax.transaction.Transaction;

import org.apache.cxf.common.logging.LogUtils;
import org.apache.cxf.transport.jms.JMSConfiguration;
import org.apache.cxf.transport.jms.JMSFactory;

public class PollingMessageListenerContainer extends AbstractMessageListenerContainer {
    private static final Logger LOG = LogUtils.getL7dLogger(PollingMessageListenerContainer.class);

    private ExecutorService pollers;

    private int concurrentConsumers = 1;

    private JMSConfiguration jmsConfig;
    private boolean reply;

    public PollingMessageListenerContainer(JMSConfiguration jmsConfig, boolean isReply,
                                           MessageListener listenerHandler) {
        this.jmsConfig = jmsConfig;
        this.reply = isReply;
        this.listenerHandler = listenerHandler;
    }

    public PollingMessageListenerContainer(Connection connection, Destination destination,
                                           MessageListener listenerHandler) {
        this.connection = connection;
        this.destination = destination;
        this.listenerHandler = listenerHandler;
    }

    private class Poller implements Runnable {

        @Override
        public void run() {
            while (running) {
                try (ResourceCloser closer = new ResourceCloser()) {
                    Connection connection;
                    if (jmsConfig != null && jmsConfig.isOneSessionPerConnection()) {
                        connection = closer.register(createConnection());
                    } else {
                        connection = PollingMessageListenerContainer.this.connection;
                    }
                    // Create session early to optimize performance
                    Session session = closer.register(connection.createSession(transacted, acknowledgeMode));
                    MessageConsumer consumer;
                    consumer = createConsumer(closer, connection, session);

                    while (running) {
                        Message message = consumer.receive(1000);
                        try {
                            if (message != null) {
                                listenerHandler.onMessage(message);
                            }
                            if (session.getTransacted()) {
                                session.commit();
                            }
                        } catch (Throwable e) {
                            if (e instanceof Exception) {
                                LOG.log(Level.WARNING, "Exception while processing jms message in cxf. Rolling back",
                                        (Exception)e);
                            }
                            safeRollBack(session, e);
                        }
                    }
                } catch (Throwable e) {
                    if (e instanceof Exception) {
                        LOG.log(Level.WARNING, "Unexpected exception. Restarting session and consumer", (Exception)e);
                    }
                }
            }

        }
        
        private void safeRollBack(Session session, Throwable e) {
            try {
                if (session.getTransacted()) {
                    session.rollback();
                }
            } catch (Throwable e1) {
                LOG.log(Level.WARNING, "Rollback of Local transaction failed", e1);
            }
        }

    }
    
    private class XAPoller implements Runnable {

        @Override
        public void run() {
            while (running) {
                try (ResourceCloser closer = new ResourceCloser()) {
                    final Transaction externalTransaction = transactionManager.getTransaction();
                    if ((externalTransaction != null) && (externalTransaction.getStatus() == Status.STATUS_ACTIVE)) {
                        LOG.log(Level.SEVERE, "External transactions are not supported in XAPoller");
                        throw new IllegalStateException("External transactions are not supported in XAPoller");
                    }
                    transactionManager.begin();

                    try {
                        Connection connection;
                        if (jmsConfig != null && jmsConfig.isOneSessionPerConnection()) {
                            connection = closer.register(createConnection());
                        } else {
                            connection = PollingMessageListenerContainer.this.connection;
                        }

                        /*
                         * Create session inside transaction to give it the
                         * chance to enlist itself as a resource
                         */
                        Session session = closer.register(connection.createSession(transacted, acknowledgeMode));
                        MessageConsumer consumer;
                        consumer = createConsumer(closer, connection, session);

                        Message message = consumer.receive(1000);

                        if (message != null) {
                            listenerHandler.onMessage(message);
                        }
                        transactionManager.commit();
                    } catch (Throwable e) {
                        LOG.log(Level.WARNING, "Exception while processing jms message in cxf. Rolling back", e);
                        safeRollBack();
                    }
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Unexpected exception. Restarting session and consumer", e);
                }

            }

        }
        
        private void safeRollBack() {
            try {
                transactionManager.rollback();
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Rollback of XA transaction failed", e);
            }
        }

    }
    
    private MessageConsumer createConsumer(final ResourceCloser closer, final Connection connection, final Session session) throws JMSException {
        final MessageConsumer consumer;
        if (jmsConfig != null && jmsConfig.isOneSessionPerConnection()) {
            Destination destination;
            if (!isReply()) {
                destination = jmsConfig.getTargetDestination(session);
            } else {
                destination = jmsConfig.getReplyDestination(session);
            }
            consumer = closer.register(createConsumer(destination, session));
            connection.start();
        } else {
            consumer = closer.register(createConsumer(session));
        }
        return consumer;
    }

    private MessageConsumer createConsumer(Session session) throws JMSException {
        return createConsumer(this.destination, session);
    }

    private MessageConsumer createConsumer(Destination destination, Session session) throws JMSException {
        if (durableSubscriptionName != null && destination instanceof Topic) {
            return session.createDurableSubscriber((Topic)destination, durableSubscriptionName,
                                                   messageSelector, pubSubNoLocal);
        } else {
            return session.createConsumer(destination, messageSelector);
        }
    }

    private boolean isReply() {
        return reply;
    }

    private Connection createConnection() {
        try {
            return JMSFactory.createConnection(jmsConfig);
        } catch (JMSException e) {
            throw JMSUtil.convertJmsException(e);
        }
    }

    @Override
    public void start() {
        if (running) {
            return;
        }
        running = true;
        pollers = Executors.newFixedThreadPool(concurrentConsumers);
        for (int c = 0; c < concurrentConsumers; c++) {
            Runnable poller = (transactionManager != null) ? new XAPoller() : new Poller(); 
            pollers.execute(poller);
        }
    }

    @Override
    public void stop() {
        LOG.fine("Shuttting down " + this.getClass().getSimpleName());
        if (!running) {
            return;
        }
        running = false;
        pollers.shutdown();
        try {
            pollers.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // Ignore
        }
        pollers.shutdownNow();
        pollers = null;
    }

    @Override
    public void shutdown() {
        stop();
    }

    public void setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
    }
}
