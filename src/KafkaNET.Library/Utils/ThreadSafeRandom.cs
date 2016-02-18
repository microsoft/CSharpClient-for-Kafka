// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Kafka.Client.Utils
{
    using System;

    public class ThreadSafeRandom
    {
        private readonly Random random = new Random();
        private readonly object useLock = new object();

        public int Next(int ceiling)
        {
            lock (useLock)
            {
                return random.Next(ceiling);
            }
        }

        public int Next()
        {
            lock (useLock)
            {
                return random.Next();
            }
        }
    }
}
