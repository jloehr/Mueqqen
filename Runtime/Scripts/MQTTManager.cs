// Copyright (c) 2018 Julian Löhr
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
            Connection = new MqttClient(BrokerHostname, Port, false, MqttSslProtocols.None, null, null);
#endif
            Connection.MqttMsgPublishReceived += OnMqttMsgPublishReceived;

            if (AutoConnect)
            {
                TryToConnect();
            }
        }

        public void TryToConnect()
        {
            if (ConnectingRoutine != null || Connection.IsConnected)
            {
                return;
            }

            ConnectingRoutine = StartCoroutine(Connect());
        }

        public IEnumerator Connect()
        {
            while (!Connection.IsConnected)
            {
                try
                {
                    Connection.Connect(Guid.NewGuid().ToString());
                }
                catch(Exception e)
                {
                    Debug.Log("Unable to connect to " + BrokerHostname);
                    Debug.LogException(e);
                }

                yield return new WaitForSeconds(ConnectionAttemptInterval);
            }

            SubscribeAll();
            ConnectingRoutine = null;
        }
        #endregion

        #region Destruction and Disconnect
        protected override void OnDestroy()
        {
            Connection.MqttMsgPublishReceived -= OnMqttMsgPublishReceived;

            Disconnect();

            // Make sure MQTTClient Thread is not within our event handler;
            lock (MessageQueue) { };
            base.OnDestroy();
        }

        public void Disconnect()
        {
            if (ConnectingRoutine != null)
            {
                StopCoroutine(ConnectingRoutine);
            }

            try
            {
                Connection.Disconnect();
            }
            catch { };
        }
        #endregion

        #region Subscribe, Unsubscribe and Publish
        public void Subscribe(string Topic, Action<string, byte[]> Callback)
        {
            Subscribe(new string[] { Topic }, new Action<string, byte[]>[] { Callback });
        }

        public void Subscribe(string[] Topics, Action<string, byte[]>[] Callbacks)
        {
            int TopicsCount = Math.Min(Topics.Length, Callbacks.Length);
            List<string> NewSubscriptions = new List<string>();
            
            for (int i = 0; i < TopicsCount; i++)
            {
                MQTTSubscription Subscription;
                if(Subscriptions.TryGetValue(Topics[i], out Subscription))
                {
                    Subscription.AddCallback(Callbacks[i]);
                }
                else
                {
                    Subscriptions.Add(Topics[i], new MQTTSubscription(Topics[i], Callbacks[i]));
                    NewSubscriptions.Add(Topics[i]);
                }
            }

            if(NewSubscriptions.Count > 0)
            {
                Subscribe(NewSubscriptions.ToArray());
            }
        }

        private void SubscribeAll()
        {
            if (Subscriptions.Count == 0)
                return;

            string[] Topics = new string[Subscriptions.Count];
            Subscriptions.Keys.CopyTo(Topics, 0);
            Subscribe(Topics);
        }

        private void Subscribe(string[] Topics)
        {
            if (Connection.IsConnected)
            {
                byte[] QoS = new byte[Topics.Length];
                for(int i = 0; i < QoS.Length; i++)
                {
                    QoS[i] = MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE;
                }

                Connection.Subscribe(Topics, QoS);
            }
        }

        public void Unsubscribe(string Topic, Action<string, byte[]> Callback)
        {
            Unsubscribe(new string[] { Topic }, new Action<string, byte[]>[] { Callback });
        }

        public void Unsubscribe(string[] Topics, Action<string, byte[]>[] Callbacks)
        {
            int TopicsCount = Math.Min(Topics.Length, Callbacks.Length);
            List<string> ObsoleteSubscriptions = new List<string>();

            for (int i = 0; i < TopicsCount; i++)
            {
                MQTTSubscription Subscription;
                if (Subscriptions.TryGetValue(Topics[i], out Subscription))
                {
                    Subscription.RemoveCallback(Callbacks[i]);
                    if(Subscription.HasNoSubscribers)
                    {
                        ObsoleteSubscriptions.Add(Topics[i]);
                        Subscriptions.Remove(Topics[i]);
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
            switch(QoS)
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
            if (Connection.IsConnected)
            {
                Connection.Publish(Topic, Data, ConvertQoS(QoS), Retain);
            }
        }
        #endregion

        #region Update
        private void Update()
        {
            if (Connection.IsConnected)
            {
                ProcessQueue();
            }
            else if (AutoConnect)
            {
                TryToConnect();
            }
        }

        private void OnMqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            lock (MessageQueue)
            {
                MessageQueue.Enqueue(new Message(e.Topic, e.Message));
            }
        }

        private void ProcessQueue()
        {
            lock (MessageQueue)
            {
                while (MessageQueue.Count > 0)
                {
                    ProcessMessage(MessageQueue.Dequeue());
                }
            }
        }
        
        private void ProcessMessage(Message NewMessage)
        {
            foreach (MQTTSubscription Endpoint in Subscriptions.Values)
            {
                Endpoint.ProcessMessage(NewMessage.Topic, NewMessage.Data);
            }
        }
        #endregion
    }
}
