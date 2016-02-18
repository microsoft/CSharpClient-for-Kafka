//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace Kafka.Client.Utils
//{
//    using System.Diagnostics;

//    public static class Logger
//    {
//        public static bool IsDebugEnabled { get; set; }

//        public static void Error(string message, params object[] formatArgs)
//        {
//            Trace.TraceError(message, formatArgs);
//        }

//        public static void Warn(string message, params object[] formatArgs)
//        {
//            Trace.TraceWarning(message, formatArgs);
//        }

//        public static void Info(string message, params object[] formatArgs)
//        {
//            Trace.TraceInformation(message, formatArgs);
//        }

//        public static void Verbose(string message, params object[] formatArgs)
//        {
//            Trace.WriteLine(string.Format(message, formatArgs), "Verbose");
//        }

//        public static void Debug(string message, params object[] formatArgs)
//        {
//            if (IsDebugEnabled)
//            {
//                Trace.WriteLine(string.Format(message, formatArgs), "Debug");
//            }
//        }
//    }
//}
