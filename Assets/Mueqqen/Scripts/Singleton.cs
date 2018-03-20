// Copyright (c) 2018 Julian Löhr
// Licensed under the MIT license.
using UnityEngine;

namespace Mueqqen
{ 
    public class Singleton<T> : MonoBehaviour where T: Singleton<T>
    {
        private static T SingletonInstance;

        public static T Instance
        {
            get {
                return SingletonInstance;
            }
        }

        protected virtual void Awake()
        {
            if(SingletonInstance == null)
            {
                SingletonInstance = this as T;
                DontDestroyOnLoad(gameObject);
            }
            else
            {
                Debug.LogErrorFormat("Instantiation of a second singleton of type {0}.", typeof(T).Name);
            }
        }
        
        protected virtual void OnDestroy()
        {
            if(SingletonInstance == this)
            {
                SingletonInstance = null;
            }

        }
    }
}