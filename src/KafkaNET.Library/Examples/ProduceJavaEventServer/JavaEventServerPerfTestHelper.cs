// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using Microsoft.KafkaNET.Library.Util;
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Cache;
    using System.Net.Security;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal class JavaEventServerPerfTestHelper : PerfTestHelperBase
    {
        internal static log4net.ILog Logger2 = log4net.LogManager.GetLogger(typeof(JavaEventServerPerfTestHelper));

        private static ArrayList messages = new ArrayList();
        private static int bufferLenth = 0;
        private static JavaEventServerPerfTestHelperOptions perfTestOption = null;
        private static String url = string.Empty;

        static String RandomString(int len)
        {
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var random = new Random();
            var result = new string(
                Enumerable.Repeat(chars, len)
                          .Select(s => s[random.Next(s.Length)])
                          .ToArray());
            return result;
        }

        static HttpWebRequest CreateRequest(String url, ArrayList messages, int bufferLenth, int threadId)
        {
            BatchedMessages batchedMessages = new BatchedMessages(bufferLenth);
            foreach (Message msg in messages)
            {
                if (!batchedMessages.PutMessage(msg))
                {
                    throw new Exception("Can't put message");
                }
            }

            byte[] ret = batchedMessages.GetData();

            while (true)
            {
                try
                {
                    // Create a request for the URL. 
                    HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                    request.KeepAlive = true;
                    request.Method = "POST";
                    // If required by the server, set the credentials.
                    request.Credentials = CredentialCache.DefaultCredentials;

                    //request.ContentLength = ret.Length;
                    request.ContentType = "application/x-www-urlencoded";
                    using (var requestStream = request.GetRequestStream())
                    {
                        requestStream.Write(ret, 0, ret.Length);
                    }
                    //request.Timeout = 150000;
                    request.CachePolicy = new RequestCachePolicy(RequestCacheLevel.BypassCache);
                    return request;
                }
                catch (Exception ex)
                {
                    Logger2.ErrorFormat("Exception while create request: Thread {0} Will sleep {1} ms,  {2}", threadId, perfTestOption.SleepInMSWhenException, ExceptionUtil.GetExceptionDetailInfo(ex));
                    Thread.Sleep(perfTestOption.SleepInMSWhenException);
                }

            }
        }

        static ResponseStatus GetResponse(HttpWebRequest request)
        {
            ResponseStatus responseStatus = new ResponseStatus();
            string output = string.Empty;
            int retry = 0;
            int maxRetry = 1;
            while (retry < maxRetry)
            {
                retry++;
                try
                {
                    using (WebResponse webresponse = request.GetResponse())
                    {
                        using (var stream = new StreamReader(webresponse.GetResponseStream(), Encoding.GetEncoding(1252)))
                        {
                            output = stream.ReadToEnd();
                            responseStatus.success = true;
                            responseStatus.info = output;
                        }
                    }
                    return responseStatus;
                }
                catch (WebException ex)
                {
                    responseStatus.success = false;
                    if (ex.Response != null)
                    {
                        using (var stream = new StreamReader(ex.Response.GetResponseStream()))
                        {
                            output = stream.ReadToEnd();
                            responseStatus.info = string.Format("WebException: {0}  {1} ", ex.Status, output);
                        }
                    }
                    else
                        responseStatus.info = string.Format("WebException:{0}  WebResponse is null. ", ex.Status);
                    if (ex.Status == WebExceptionStatus.ConnectionClosed)
                    {
                        maxRetry = 2;
                        responseStatus.everRetryForConnectionClosed = true;
                        Thread.Sleep(10);
                    }

                }
                catch (Exception e)
                {
                    responseStatus.success = false;
                    responseStatus.info = string.Format("Exception:{0}  {1}  {2}", e.Message, e.GetType(), ExceptionUtil.GetExceptionDetailInfo(e));
                }
            }
            return responseStatus;
        }
        internal static bool AcceptAllCertifications(object sender,
            X509Certificate certification, X509Chain chain,
            System.Net.Security.SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }

        internal void Run(JavaEventServerPerfTestHelperOptions evetServerPerfTestOptions)
        {
            produceOption = evetServerPerfTestOptions;
            perfTestOption = evetServerPerfTestOptions;
            url = evetServerPerfTestOptions.EventServerFullAddress + "/eventserver?type=" + evetServerPerfTestOptions.Topic + "&sendtype=" + evetServerPerfTestOptions.SendType;
            if (url.ToLowerInvariant().StartsWith("https:"))
            {
                ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(AcceptAllCertifications);
            }

            byte[] bKey = null;
            byte[] bVal = null;
            for (int i = 0; i < evetServerPerfTestOptions.MessageCountPerBatch; i++)
            {
                String key = RandomString(56);
                bKey = System.Text.Encoding.UTF8.GetBytes(key);
                String val = RandomString(evetServerPerfTestOptions.MessageSize);
                bVal = System.Text.Encoding.UTF8.GetBytes(val);
                messages.Add(new Message(bKey, bVal));
            }

            bufferLenth = evetServerPerfTestOptions.MessageCountPerBatch * (8 + bKey.Length + bVal.Length);

            Thread[] threads = new Thread[evetServerPerfTestOptions.ThreadCount];
            AutoResetEvent[] autos = new AutoResetEvent[evetServerPerfTestOptions.ThreadCount];
            threadPareamaters = new PerfTestThreadParameter[evetServerPerfTestOptions.ThreadCount];

            Logger.InfoFormat("start send {0} ", DateTime.Now);

            stopWatch.Restart();
            for (int i = 0; i < evetServerPerfTestOptions.ThreadCount; i++)
            {
                AutoResetEvent Auto = new AutoResetEvent(false);
                PerfTestThreadParameter p = new PerfTestThreadParameter();
                p.ThreadId = i;
                p.SpeedConstrolMBPerSecond = (evetServerPerfTestOptions.SpeedConstrolMBPerSecond * 1.0) / evetServerPerfTestOptions.ThreadCount;
                p.EventOfFinish = Auto;
                Thread t = new Thread(new ParameterizedThreadStart(RunOneThread));
                t.Start(p);
                autos[i] = Auto;
                threadPareamaters[i] = p;
            }

            WaitHandle.WaitAll(autos);
            stopWatch.Stop();
            Statistics();
        }

        private void RunOneThread(object parameter)
        {
            PerfTestThreadParameter p = (PerfTestThreadParameter)parameter;
            DateTime startTime = DateTime.UtcNow;

            while (true)
            {
                p.RequestIndex++;

                HttpWebRequest request = CreateRequest(url, messages, bufferLenth, p.ThreadId);
                p.StopWatchForSuccessRequest.Restart();
                ResponseStatus responseStatus = GetResponse(request);
                p.StopWatchForSuccessRequest.Stop();
                p.SuccRecordsTimeInms += p.StopWatchForSuccessRequest.ElapsedMilliseconds;

                if (responseStatus.success == false)
                {
                    Logger2.ErrorFormat("Thread: {0}  Request: {1}  {2} everRetryForConnectionClosed:{4} Will sleep {3}ms", p.ThreadId, p.RequestIndex, responseStatus.info, perfTestOption.SleepInMSWhenException, responseStatus.everRetryForConnectionClosed);
                    p.RequestFail++;
                    p.RequestFailAfterRetry++;
                    Thread.Sleep(perfTestOption.SleepInMSWhenException);
                }
                else
                {
                    p.RequestSucc++;
                    p.SentSuccBytes += bufferLenth;
                    if (responseStatus.everRetryForConnectionClosed)
                    {
                        p.RequestFail++;
                        Logger2.ErrorFormat("Thread: {0}  Request: {1}  {2} everRetryForConnectionClosed:{3} Got success after retry.", p.ThreadId, p.RequestIndex, responseStatus.info, responseStatus.everRetryForConnectionClosed);
                    }
                }

                if (p.ThreadId == 0 && p.RequestIndex % 200 == 0)
                    Statistics();
                if (perfTestOption.BatchCount > 0 && p.RequestIndex >= perfTestOption.BatchCount)
                    break;

                if (p.SpeedConstrolMBPerSecond > 0.0)
                {
                    while (true)
                    {
                        double expectedSuccBytes = (DateTime.UtcNow - startTime).TotalSeconds * p.SpeedConstrolMBPerSecond * 1024 * 1024;
                        if (p.SentSuccBytes > expectedSuccBytes)
                        {
                            Thread.Sleep(20);
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }

            p.EventOfFinish.Set();
        }

        private class BatchedMessages : IDisposable
        {
            MemoryStream stream;
            BinaryWriter writer;
            int limit = 0;
            int size = 0;

            internal BatchedMessages(int limit)
            {
                this.stream = new MemoryStream(limit);
                this.writer = new BinaryWriter(stream);
                this.limit = limit;
            }
            internal BatchedMessages() : this(102400) { }

            internal bool PutMessage(Message msg)
            {
                return PutMessage(msg.GetKey(), msg.GetVal());
            }

            internal bool PutMessage(byte[] val)
            {
                return PutMessage(null, val);
            }

            internal bool PutMessage(byte[] key, byte[] val)
            {
                if (key == null)
                {
                    if (size + 8 + val.Length > limit) return false;
                }
                else
                {
                    if (size + 8 + key.Length + val.Length > limit) return false;
                }

                if (val == null)
                {
                    return false;
                }

                try
                {
                    if (key == null)
                    {
                        writer.Write(0);
                    }
                    else
                    {
                        writer.Write(ReverseBytes((uint)key.Length));
                        writer.Write(key);
                    }
                    writer.Write(ReverseBytes((uint)val.Length));
                    writer.Write(val);
                }
                catch (Exception ex)
                {
                    throw ex;
                }

                if (key == null)
                {
                    size += 8 + val.Length;
                }
                else
                {
                    size += 8 + key.Length + val.Length;
                }
                return true;
            }

            internal byte[] GetData()
            {
                byte[] bytes = new byte[stream.Position];
                stream.Seek(0, SeekOrigin.Begin);
                stream.Read(bytes, 0, bytes.Length);
                return bytes;
            }

            public void Dispose()
            {
                if (stream != null)
                    stream.Dispose();
                this.Dispose();
            }

            private uint ReverseBytes(uint value)
            {
                return (value & 0x000000FFU) << 24 | (value & 0x0000FF00U) << 8 |
                       (value & 0x00FF0000U) >> 8 | (value & 0xFF000000U) >> 24;
            }
        }

        private class ResponseStatus
        {
            internal bool success;
            internal int[] indexes;
            internal string info;
            internal bool everRetryForConnectionClosed;

            internal ResponseStatus(bool success, int[] indexes, string info)
            {
                this.success = success;
                this.indexes = indexes;
                this.info = info;
            }

            internal ResponseStatus() { }
        }

        private class Message
        {
            byte[] key;
            byte[] val;

            internal Message(byte[] key, byte[] val)
            {
                this.key = key;
                this.val = val;
            }

            internal byte[] GetKey()
            {
                return key;
            }

            internal byte[] GetVal()
            {
                return val;
            }
        }
    }
}

