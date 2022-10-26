// Copyright (c) 2022 Julian Löhr
// Licensed under the MIT license.
using UnityEngine;

namespace Mueqqen
{
    [RequireComponent(typeof(MQTTManager))]
    public class MQTTManagerSingleton : MonoBehaviour
    {
        public static MQTTManager Instance;

        private MQTTManager ThisInstance;  

        private  void Awake()
        {
            if (Instance == null)
            {
                Instance = this.GetComponent<MQTTManager>();
                DontDestroyOnLoad(this.gameObject);
            }
            else
            {
                Debug.LogErrorFormat("Instantiation of a second singleton of type {0}.", nameof(MQTTManager));
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
