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

namespace Kafka.Client
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;

    public static class KafkaStopWatch
    {
        private static Stopwatch stopWatch;
        private static long memUsage;

        public static void Start()
        {
            stopWatch = Stopwatch.StartNew();
            memUsage = GC.GetTotalMemory(false);
        }

        public static void Checkpoint(string msg)
        {
            stopWatch.Stop();
            var current = GC.GetTotalMemory(false) - memUsage;
            var result = stopWatch.Elapsed.TotalMilliseconds;
            Console.WriteLine(string.Format("{0}: {1,-6:0.000}ms, {2, 10:0.0}kB, {3, 10:0.0}kB", msg, result, (GC.GetTotalMemory(false) / 1000.0), (current / 1000.0)));
            memUsage = GC.GetTotalMemory(false);
            stopWatch.Restart();
        }
    }
}
