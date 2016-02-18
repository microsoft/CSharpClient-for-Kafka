/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Kafka.Client.ZooKeeperIntegration.Listeners
{
    using Kafka.Client.Consumers;
    using Kafka.Client.Utils;
    using Kafka.Client.ZooKeeperIntegration.Events;
    using log4net;
    using System;
    using System.Globalization;
    using System.Reflection;
    using ZooKeeperNet;

    internal class ZKSessionExpireListener<TData> : IZooKeeperStateListener
    {
        public static log4net.ILog Logger = log4net.LogManager.GetLogger("ZKSessionExpireListener");

        public event EventHandler ZKSessionDisconnected;
        public event EventHandler ZKSessionExpired;

        private readonly string consumerIdString;
        private readonly ZKRebalancerListener<TData> loadBalancerListener;
        private readonly ZookeeperConsumerConnector zkConsumerConnector;
        private readonly ZKGroupDirs dirs;
        private readonly TopicCount topicCount;

        public ZKSessionExpireListener(ZKGroupDirs dirs, string consumerIdString, TopicCount topicCount, ZKRebalancerListener<TData> loadBalancerListener, ZookeeperConsumerConnector zkConsumerConnector)
        {
            this.consumerIdString = consumerIdString;
            this.loadBalancerListener = loadBalancerListener;
            this.zkConsumerConnector = zkConsumerConnector;
            this.dirs = dirs;
            this.topicCount = topicCount;
        }

        /// <summary>
        /// Called when the ZooKeeper connection state has changed.
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperStateChangedEventArgs"/> instance containing the event data.</param>
        /// <remarks>
        /// Do nothing, since zkclient will do reconnect for us.
        /// </remarks>
        public void HandleStateChanged(ZooKeeperStateChangedEventArgs args)
        {
            Guard.NotNull(args, "args");
            Guard.Assert<ArgumentException>(() => args.State != KeeperState.Unknown);

            if (args.State != KeeperState.Disconnected)
            {
                Logger.Info("ZK session disconnected; shutting down fetchers and resetting state");

                // Notify listeners that ZK session has disconnected
                OnZKSessionDisconnected(EventArgs.Empty);

                // Shutdown fetcher threads until we reconnect
                this.loadBalancerListener.ShutdownFetchers();
            }
            else if (args.State == KeeperState.SyncConnected)
            {
                Logger.Info("Performing rebalancing. ZK session has reconnected");

                // Restart fetcher threads via rebalance in case we no longer own a partition
                this.loadBalancerListener.AsyncRebalance();
            }
        }

        /// <summary>
        /// Called after the ZooKeeper session has expired and a new session has been created.
        /// </summary>
        /// <param name="args">The <see cref="Kafka.Client.ZooKeeperIntegration.Events.ZooKeeperSessionCreatedEventArgs"/> instance containing the event data.</param>
        /// <remarks>
        /// You would have to re-create any ephemeral nodes here.
        /// Explicitly trigger load balancing for this consumer.
        /// </remarks>
        public void HandleSessionCreated(ZooKeeperSessionCreatedEventArgs args)
        {
            Guard.NotNull(args, "args");

            // Notify listeners that ZK session has expired
            OnZKSessionExpired(EventArgs.Empty);

            Logger.InfoFormat("ZK session expired; release old broker partition ownership; re-register consumer {0}", this.consumerIdString);
            this.loadBalancerListener.ResetState();
            this.zkConsumerConnector.RegisterConsumerInZk(this.dirs, this.consumerIdString, this.topicCount);

            Logger.Info("Performing rebalancing. ZK session has previously expired and a new session has been created");
            this.loadBalancerListener.AsyncRebalance();
        }

        protected virtual void OnZKSessionDisconnected(EventArgs args)
        {
            try
            {
                EventHandler handler = ZKSessionDisconnected;
                if (handler != null)
                {
                    handler(this, args);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("Exception occurred within event handler for ZKSessionDisconnected event: " + ex.Message);
            }
        }

        protected virtual void OnZKSessionExpired(EventArgs args)
        {
            try
            {
                EventHandler handler = ZKSessionExpired;
                if (handler != null)
                {
                    handler(this, args);
                }
            }
            catch (Exception ex)
            {
                Logger.Error("Exception occurred within event handler for ZKSessionExpired event: " + ex.Message);
            }
        }
    }
}