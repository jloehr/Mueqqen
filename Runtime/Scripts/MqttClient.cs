// Copyright (c) 2022 Julian Löhr
// Licensed under the MIT license.
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;

using UnityEngine;

using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace Mueqqen
{
    public class MqttClient : MonoBehaviour
    {
        public enum PublishType { FireAndForget, FireTillAck, ExactlyOnce };

        public static MqttClient Instance
        {
            get { return MqttSingleton.Instance; }
        }

        private struct Message
        {
            public string Topic;
            public byte[] Data;

            public Message(string Topic, byte[] Data)
            {
                this.Topic = Topic;
                this.Data = Data;
            }
        }

        public string BrokerHostname = "mqtt-broker";
        public int Port = 1883;
        public bool AutoConnect = true;
        public float ConnectionAttemptInterval = 1f;

        private MqttFactory MqttFactory = new MqttFactory();
        private IMqttClient Connection = null;
        private Coroutine ConnectingRoutine = null;

        private Queue<Message> MessageQueue = new Queue<Message>();
        private Dictionary<string, MqttSubscription> Subscriptions = new Dictionary<string, MqttSubscription>();

        #region Setup and Connecting
        private void Awake()
        {
            this.Connection = this.MqttFactory.CreateMqttClient();
            this.Connection.ApplicationMessageReceivedAsync += this.ApplicationMessageReceivedAsync;

            if (this.AutoConnect)
            {
                this.TryToConnect();
            }
        }

        public void TryToConnect()
        {
            if (this.ConnectingRoutine != null || this.Connection.IsConnected)
            {
                return;
            }

            this.ConnectingRoutine = this.StartCoroutine(this.Connect());
        }

        public IEnumerator Connect()
        {
            MqttClientOptions mqttClientOptions = this.MqttFactory.CreateClientOptionsBuilder().WithTcpServer(this.BrokerHostname, this.Port).Build();

            while (!this.Connection.IsConnected)
            {
                Task<MqttClientConnectResult> connectTask = this.Connection.ConnectAsync(mqttClientOptions);

                yield return new WaitUntil(() => connectTask.IsCompleted);

                if (connectTask.IsFaulted)
                {
                    yield return new WaitForSeconds(this.ConnectionAttemptInterval);
                }
            }

            this.SubscribeAll();
            this.ConnectingRoutine = null;
        }
        #endregion

        #region Destruction and Disconnect
        protected void OnDestroy()
        {
            this.Connection.ApplicationMessageReceivedAsync -= this.ApplicationMessageReceivedAsync;

            this.Disconnect();

            // Make sure MQTTClient Thread is not within our event handler;
            lock (this.MessageQueue) { };
        }

        public void Disconnect()
        {
            if (this.ConnectingRoutine != null)
            {
                this.StopCoroutine(this.ConnectingRoutine);
            }

            // Ungraceful Disconnect for now
            this.Connection.Dispose();
        }
        #endregion

        #region Subscribe, Unsubscribe and Publish
        public void Subscribe(string Topic, Action<string, byte[]> Callback)
        {
            this.Subscribe(new string[] { Topic }, new Action<string, byte[]>[] { Callback });
        }

        public void Subscribe(string[] Topics, Action<string, byte[]>[] Callbacks)
        {
            int TopicsCount = Math.Min(Topics.Length, Callbacks.Length);
            List<string> NewSubscriptions = new List<string>();

            for (int i = 0; i < TopicsCount; i++)
            {
                if (this.Subscriptions.TryGetValue(Topics[i], out MqttSubscription Subscription))
                {
                    Subscription.AddCallback(Callbacks[i]);
                }
                else
                {
                    this.Subscriptions.Add(Topics[i], new MqttSubscription(Topics[i], Callbacks[i]));
                    NewSubscriptions.Add(Topics[i]);
                }
            }

            if (NewSubscriptions.Count > 0)
            {
                this.Subscribe(NewSubscriptions.ToArray());
            }
        }

        private void SubscribeAll()
        {
            if (this.Subscriptions.Count == 0)
                return;

            string[] Topics = new string[this.Subscriptions.Count];
            this.Subscriptions.Keys.CopyTo(Topics, 0);
            this.Subscribe(Topics);
        }

        private void Subscribe(string[] Topics)
        {
            if (this.Connection.IsConnected)
            {
                MqttClientSubscribeOptionsBuilder mqttSubscribeOptions = this.MqttFactory.CreateSubscribeOptionsBuilder();

                foreach(string Topic in Topics)
                {
                    mqttSubscribeOptions.WithTopicFilter(Topic);
                }

                this.Connection.SubscribeAsync(mqttSubscribeOptions.Build());
            }
        }

        public void Unsubscribe(string Topic, Action<string, byte[]> Callback)
        {
            this.Unsubscribe(new string[] { Topic }, new Action<string, byte[]>[] { Callback });
        }

        public void Unsubscribe(string[] Topics, Action<string, byte[]>[] Callbacks)
        {
            int TopicsCount = Math.Min(Topics.Length, Callbacks.Length);
            List<string> ObsoleteSubscriptions = new List<string>();

            for (int i = 0; i < TopicsCount; i++)
            {
                if (this.Subscriptions.TryGetValue(Topics[i], out MqttSubscription Subscription))
                {
                    Subscription.RemoveCallback(Callbacks[i]);
                    if (Subscription.HasNoSubscribers)
                    {
                        ObsoleteSubscriptions.Add(Topics[i]);
                        this.Subscriptions.Remove(Topics[i]);
                    }
                }
                else
                {
                    Debug.LogFormat("MQTT Unsubscribe: {0} not found in subscription list. Check for duplicate unsubcribe.", Topics[i]);
                }
            }
        }

        private MqttQualityOfServiceLevel ConvertQoS(PublishType QoS)
        {
            switch (QoS)
            {
                case PublishType.FireAndForget:
                    return MqttQualityOfServiceLevel.AtMostOnce;
                case PublishType.FireTillAck:
                    return MqttQualityOfServiceLevel.AtLeastOnce;
                case PublishType.ExactlyOnce:
                    return MqttQualityOfServiceLevel.ExactlyOnce;
                default:
                    Debug.LogError("Unknown QoS Type");
                    return MqttQualityOfServiceLevel.AtMostOnce;
            }
        }

        public void Publish(string Topic, byte[] Data, bool Retain, PublishType QoS)
        {
            if (this.Connection.IsConnected)
            {
                this.Connection.PublishBinaryAsync(Topic, Data, this.ConvertQoS(QoS), Retain);
            }
        }
        #endregion

        #region Update
        private void Update()
        {
            if (this.Connection.IsConnected)
            {
                this.ProcessQueue();
            }
            else if (this.AutoConnect)
            {
                this.TryToConnect();
            }
        }

        private Task ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            lock (this.MessageQueue)
            {
                this.MessageQueue.Enqueue(new Message(arg.ApplicationMessage.Topic, arg.ApplicationMessage.Payload));
            }

            return Task.CompletedTask;
        }

        private void ProcessQueue()
        {
            lock (this.MessageQueue)
            {
                while (this.MessageQueue.Count > 0)
                {
                    this.ProcessMessage(this.MessageQueue.Dequeue());
                }
            }
        }

        private void ProcessMessage(Message NewMessage)
        {
            foreach (MqttSubscription Endpoint in this.Subscriptions.Values)
            {
                Endpoint.ProcessMessage(NewMessage.Topic, NewMessage.Data);
            }
        }
        #endregion
    }
}
