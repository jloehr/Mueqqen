// Copyright (c) 2022 Julian Löhr
// Licensed under the MIT license.
using UnityEngine;

namespace Mueqqen
{
    [RequireComponent(typeof(MqttClient))]
    public class MqttSingleton : MonoBehaviour
    {
        public static MqttClient Instance;

        private MqttClient ThisInstance;  

        private  void Awake()
        {
            if (Instance == null)
            {
                Instance = this.GetComponent<MqttClient>();
                DontDestroyOnLoad(this.gameObject);
            }
            else
            {
                Debug.LogErrorFormat("Instantiation of a second singleton of type {0}.", nameof(MqttClient));
            }
        }

        private void OnDestroy()
        {
            if (this.ThisInstance == Instance)
            {
                Instance = null;
            }
        }
    }
}
