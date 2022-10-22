// Copyright (c) 2022 Julian Löhr
// Licensed under the MIT license.
using System;
using System.Collections;
using System.Collections.Generic;

using UnityEngine;

using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace Mueqqen
{
    public class MQTTManager : Singleton<MQTTManager>
    {
        public enum PublishType { FireAndForget, FireTillAck, ExactlyOnce };

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

        private MqttClient Connection = null;
        private Coroutine ConnectingRoutine = null;

        private Queue<Message> MessageQueue = new Queue<Message>();
        private Dictionary<string, MQTTSubscription> Subscriptions = new Dictionary<string, MQTTSubscription>();

        #region Setup and Connecting
        protected override void Awake()
        {
            base.Awake();
#if UNITY_WSA && !UNITY_EDITOR
            Connection = new MqttClient(BrokerHostname, Port, false, MqttSslProtocols.None);    
#else
            this.Connection = new MqttClient(this.BrokerHostname, this.Port, false, MqttSslProtocols.None, null, null);
#endif
            this.Connection.MqttMsgPublishReceived += this.OnMqttMsgPublishReceived;

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
            while (!this.Connection.IsConnected)
            {
                try
                {
                    this.Connection.Connect(Guid.NewGuid().ToString());
                }
                catch (Exception e)
                {
                    Debug.Log("Unable to connect to " + this.BrokerHostname);
                    Debug.LogException(e);
                }

                yield return new WaitForSeconds(this.ConnectionAttemptInterval);
            }

            this.SubscribeAll();
            this.ConnectingRoutine = null;
        }
        #endregion

        #region Destruction and Disconnect
        protected override void OnDestroy()
        {
            this.Connection.MqttMsgPublishReceived -= this.OnMqttMsgPublishReceived;

            this.Disconnect();

            // Make sure MQTTClient Thread is not within our event handler;
            lock (this.MessageQueue) { };
            base.OnDestroy();
        }

        public void Disconnect()
        {
            if (this.ConnectingRoutine != null)
            {
                this.StopCoroutine(this.ConnectingRoutine);
            }

            try
            {
                this.Connection.Disconnect();
            }
            catch { };
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
                if (this.Subscriptions.TryGetValue(Topics[i], out MQTTSubscription Subscription))
                {
                    Subscription.AddCallback(Callbacks[i]);
                }
                else
                {
                    this.Subscriptions.Add(Topics[i], new MQTTSubscription(Topics[i], Callbacks[i]));
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
                byte[] QoS = new byte[Topics.Length];
                for (int i = 0; i < QoS.Length; i++)
                {
                    QoS[i] = MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE;
                }

                this.Connection.Subscribe(Topics, QoS);
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
                if (this.Subscriptions.TryGetValue(Topics[i], out MQTTSubscription Subscription))
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

        private byte ConvertQoS(PublishType QoS)
        {
            switch (QoS)
            {
                case PublishType.FireAndForget:
                    return MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE;
                case PublishType.FireTillAck:
                    return MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE;
                case PublishType.ExactlyOnce:
                    return MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE;
                default:
                    Debug.LogError("Unknown QoS Type");
                    return MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE;
            }
        }

        public void Publish(string Topic, byte[] Data, bool Retain, PublishType QoS)
        {
            if (this.Connection.IsConnected)
            {
                this.Connection.Publish(Topic, Data, ConvertQoS(QoS), Retain);
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

        private void OnMqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            lock (this.MessageQueue)
            {
                this.MessageQueue.Enqueue(new Message(e.Topic, e.Message));
            }
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
            foreach (MQTTSubscription Endpoint in this.Subscriptions.Values)
            {
                Endpoint.ProcessMessage(NewMessage.Topic, NewMessage.Data);
            }
        }
        #endregion
    }
}
