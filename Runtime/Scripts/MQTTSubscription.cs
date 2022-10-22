// Copyright (c) 2022 Julian Löhr
// Licensed under the MIT license.
using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Mueqqen
{
    public class MQTTSubscription
    {
        private readonly string RegexTopic;
        private readonly List<Action<string, byte[]>> Callbacks = new List<Action<string, byte[]>>();

        public bool HasNoSubscribers { get { return (this.Callbacks.Count == 0); } }

        public MQTTSubscription(string Topic, Action<string, byte[]> Callback)
        {
            this.RegexTopic = this.ConvertTopic(Topic);
            this.AddCallback(Callback);
        }

        private string ConvertTopic(string Topic)
        {
            string ConvertedTopic = "";
            bool EndWildcard = Topic.EndsWith("/#");

            // Strip '#' otherwise it will be RegexEscaped
            if (EndWildcard)
            {
                Topic = Topic.Substring(0, Topic.Length - 1);
            }

            /* Split Topic by '+' Wildcard:
             *
             * Two Capture Groups
             * First one captures empty in case Topic starts with '+/' or ungreedy at least one abitrary character.
             * I.e. that is the Topic substring.
             * Second one captures the '+' wildcard, or end of line to get the last topic substring.
             * '+' character must be preceded by start of string or '/' and succeeded by '/' to not capture 
             *  any '+' that are between abitrary characters, e.g. "Foo/Garten+Roof/Bar.
             */
            foreach (Match Match in Regex.Matches(Topic, @"((?=\+\/)|.+?)(?:$|((?<=^)|(?<=\/))\+(?=\/))"))
            {
                // Replace '+' wildcard with regex that matches arbitrary characters until '/'
                if (Match.Index != 0)
                {
                    ConvertedTopic += @"[^\/]+";
                }

                ConvertedTopic += Regex.Escape(Match.Groups[1].Value);
            }

            // In case of '#' wildcard, add generic "match all" regex
            if (EndWildcard)
            {
                ConvertedTopic += @".+";
            }

            // Wrap Topic into start and end of string, so no substring are going to be matched
            ConvertedTopic = "^" + ConvertedTopic + "$";
            return ConvertedTopic;
        }

        public void AddCallback(Action<string, byte[]> Callback)
        {
            this.Callbacks.Add(Callback);
        }

        public void RemoveCallback(Action<string, byte[]> Callback)
        {
            this.Callbacks.Remove(Callback);
        }

        public void ProcessMessage(string Topic, byte[] Payload)
        {
            if (Regex.IsMatch(Topic, this.RegexTopic))
            {
                foreach (Action<string, byte[]> Callback in this.Callbacks)
                {
                    Callback(Topic, Payload);
                }
            }
        }
    }
}
