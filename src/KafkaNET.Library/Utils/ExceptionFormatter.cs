// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace Microsoft.KafkaNET.Library.Util
{
    using System;
    using System.Collections;
    using System.Collections.ObjectModel;
    using System.Collections.Specialized;
    using System.Globalization;
    using System.IO;
    using System.Reflection;
    using System.Security;
    using System.Security.Principal;
    using System.Threading;
    using System.Xml;

    /// <summary>
    /// Represents an exception formatter that formats exception objects as XML.
    /// </summary>	
    public sealed class XmlExceptionFormatter : ExceptionFormatter
    {
        private readonly XmlWriter xmlWriter;

        /// <summary>
        /// Initializes a new instance of the <see cref="XmlExceptionFormatter"/> class using the specified <see cref="XmlWriter"/> and <see cref="Exception"/> objects.
        /// </summary>
        /// <param name="xmlWriter">The <see cref="XmlWriter"/> in which to write the XML.</param>
        /// <param name="exception">The <see cref="Exception"/> to format.</param>
        /// <param name="handlingInstanceId">The id of the handling chain.</param>
        public XmlExceptionFormatter(XmlWriter xmlWriter, Exception exception, Guid handlingInstanceId)
            : base(exception, handlingInstanceId)
        {
            if (xmlWriter == null) throw new ArgumentNullException("xmlWriter");

            this.xmlWriter = xmlWriter;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="XmlExceptionFormatter"/> class using the specified <see cref="TextWriter"/> and <see cref="Exception"/> objects.
        /// </summary>
        /// <param name="writer">The <see cref="TextWriter"/> in which to write the XML.</param>
        /// <param name="exception">The <see cref="Exception"/> to format.</param>
        /// <remarks>
        /// An <see cref="XmlTextWriter"/> with indented formatting is created from the  specified <see cref="TextWriter"/>.
        /// </remarks>
        /// <param name="handlingInstanceId">The id of the handling chain.</param>
        public XmlExceptionFormatter(TextWriter writer, Exception exception, Guid handlingInstanceId)
            : base(exception, handlingInstanceId)
        {
            if (writer == null) throw new ArgumentNullException("writer");

            XmlTextWriter textWriter = new XmlTextWriter(writer);
            textWriter.Formatting = Formatting.Indented;
            xmlWriter = textWriter;
        }

        /// <summary>
        /// Gets the underlying <see cref="XmlWriter"/> that the formatted exception is written to.
        /// </summary>
        /// <value>
        /// The underlying <see cref="XmlWriter"/> that the formatted exception is written to.
        /// </value>
        public XmlWriter Writer
        {
            get { return xmlWriter; }
        }

        /// <summary>
        /// Formats the <see cref="Exception"/> into the underlying stream.
        /// </summary>       
        public override void Format()
        {
            Writer.WriteStartElement("Exception");
            if (this.HandlingInstanceId != Guid.Empty)
            {
                Writer.WriteAttributeString(
                    "handlingInstanceId",
                    this.HandlingInstanceId.ToString("D", CultureInfo.InvariantCulture));
            }

            base.Format();

            Writer.WriteEndElement();
        }

        /// <summary>
        /// Writes the current date and time to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="datetime">The current time.</param>
        protected override void WriteDateTime(DateTime datetime)
        {
            this.WriteSingleElement("DateTime",
                datetime.ToUniversalTime().ToString("u", DateTimeFormatInfo.InvariantInfo));
        }

        /// <summary>
        /// Writes the value of the <see cref="Exception.Message"/> property to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="message">The message to write.</param>
        protected override void WriteMessage(string message)
        {
            WriteSingleElement("Message", message);
        }

        /// <summary>
        /// Writes a generic description to the <see cref="XmlWriter"/>.
        /// </summary>
        protected override void WriteDescription()
        {
            WriteSingleElement("Description", string.Format(CultureInfo.CurrentCulture, ExceptionFormatter.ExceptionWasCaught, base.Exception.GetType().FullName));
        }

        /// <summary>
        /// Writes the value of the specified help link taken
        /// from the value of the <see cref="Exception.HelpLink"/>
        /// property to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="helpLink">The exception's help link.</param>
        protected override void WriteHelpLink(string helpLink)
        {
            WriteSingleElement("HelpLink", helpLink);
        }

        /// <summary>
        /// Writes the value of the specified stack trace taken from the value of the <see cref="Exception.StackTrace"/> property to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="stackTrace">The stack trace of the exception.</param>
        protected override void WriteStackTrace(string stackTrace)
        {
            WriteSingleElement("StackTrace", stackTrace);
        }

        /// <summary>
        /// Writes the value of the specified source taken from the value of the <see cref="Exception.Source"/> property to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="source">The source of the exception.</param>
        protected override void WriteSource(string source)
        {
            WriteSingleElement("Source", source);
        }

        /// <summary>
        /// Writes the value of the <see cref="Type.AssemblyQualifiedName"/>
        /// property for the specified exception type to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="exceptionType">The <see cref="Type"/> of the exception.</param>
        protected override void WriteExceptionType(Type exceptionType)
        {
            if (exceptionType == null)
            {
                throw new ArgumentException("Exception type cannot be null");
            }

            WriteSingleElement("ExceptionType", exceptionType.AssemblyQualifiedName);
        }

        /// <summary>
        /// Writes and formats the exception and all nested inner exceptions to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="exceptionToFormat">The exception to format.</param>
        /// <param name="outerException">The outer exception. This value will be null when writing the outer-most exception.</param>
        protected override void WriteException(Exception exceptionToFormat, Exception outerException)
        {
            if (outerException != null)
            {
                Writer.WriteStartElement("InnerException");

                base.WriteException(exceptionToFormat, outerException);

                Writer.WriteEndElement();
            }
            else
            {
                base.WriteException(exceptionToFormat, outerException);
            }
        }

        /// <summary>
        /// Writes the name and value of the specified property to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="propertyInfo">The reflected <see cref="PropertyInfo"/> object.</param>
        /// <param name="value">The value of the <see cref="PropertyInfo"/> object.</param>
        protected override void WritePropertyInfo(PropertyInfo propertyInfo, object value)
        {
            if (propertyInfo == null)
            {
                throw new ArgumentException("Property info cannot be null");
            }

            string propertyValueString = ExceptionFormatter.UndefinedValue;

            if (value != null)
            {
                propertyValueString = value.ToString();
            }

            Writer.WriteStartElement("Property");
            Writer.WriteAttributeString("name", propertyInfo.Name);
            Writer.WriteString(propertyValueString);
            Writer.WriteEndElement();
        }

        /// <summary>
        /// Writes the name and value of the <see cref="FieldInfo"/> object to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="fieldInfo">The reflected <see cref="FieldInfo"/> object.</param>
        /// <param name="value">The value of the <see cref="FieldInfo"/> object.</param>
        protected override void WriteFieldInfo(FieldInfo fieldInfo, object value)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentException("Field info cannot be null");
            }

            if (value == null)
            {
                throw new ArgumentException("Value cannot be null");
            }

            string fieldValueString = ExceptionFormatter.UndefinedValue;

            if (fieldValueString != null)
            {
                fieldValueString = value.ToString();
            }

            Writer.WriteStartElement("Field");
            Writer.WriteAttributeString("name", fieldInfo.Name);
            Writer.WriteString(value.ToString());
            Writer.WriteEndElement();
        }

        /// <summary>
        /// Writes additional information to the <see cref="XmlWriter"/>.
        /// </summary>
        /// <param name="additionalInformation">Additional information to be included with the exception report</param>
        protected override void WriteAdditionalInfo(NameValueCollection additionalInformation)
        {
            if (additionalInformation == null)
            {
                throw new ArgumentException("Additional information cannot be null");
            }

            Writer.WriteStartElement("additionalInfo");

            foreach (string name in additionalInformation.AllKeys)
            {
                Writer.WriteStartElement("info");
                Writer.WriteAttributeString("name", name);
                Writer.WriteAttributeString("value", additionalInformation[name]);
                Writer.WriteEndElement();
            }

            Writer.WriteEndElement();
        }

        private void WriteSingleElement(string elementName, string elementText)
        {
            Writer.WriteStartElement(elementName);
            Writer.WriteString(elementText);
            Writer.WriteEndElement();
        }
    }

    /// <summary>
    /// Represents an exception formatter that formats exception objects as text.
    /// </summary>	
    public sealed class TextExceptionFormatter : ExceptionFormatter
    {
        private readonly TextWriter writer;
        private int innerDepth;

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="TextExceptionFormatter"/> using the specified
        /// <see cref="TextWriter"/> and <see cref="Exception"/>
        /// objects.
        /// </summary>
        /// <param name="writer">The stream to write formatting information to.</param>
        /// <param name="exception">The exception to format.</param>
        public TextExceptionFormatter(TextWriter writer, Exception exception)
            : this(writer, exception, Guid.Empty)
        { }

        /// <summary>
        /// Initializes a new instance of the 
        /// <see cref="TextExceptionFormatter"/> using the specified
        /// <see cref="TextWriter"/> and <see cref="Exception"/>
        /// objects.
        /// </summary>
        /// <param name="writer">The stream to write formatting information to.</param>
        /// <param name="exception">The exception to format.</param>
        /// <param name="handlingInstanceId">The id of the handling chain.</param>
        public TextExceptionFormatter(TextWriter writer, Exception exception, Guid handlingInstanceId)
            : base(exception, handlingInstanceId)
        {
            if (writer == null) throw new ArgumentNullException("writer");

            this.writer = writer;
        }

        /// <summary>
        /// Gets the underlying <see cref="TextWriter"/>
        /// that the current formatter is writing to.
        /// </summary>
        public TextWriter Writer
        {
            get { return this.writer; }
        }

        /// <summary>
        /// Formats the <see cref="Exception"/> into the underlying stream.
        /// </summary>
        public override void Format()
        {
            if (this.HandlingInstanceId != Guid.Empty)
            {
                this.Writer.WriteLine(
                    "HandlingInstanceID: {0}",
                    HandlingInstanceId.ToString("D", CultureInfo.InvariantCulture));
            }
            base.Format();
        }

        /// <summary>
        /// Writes a generic description to the underlying text stream.
        /// </summary>
        protected override void WriteDescription()
        {
            // An exception of type {0} occurred and was caught.
            // -------------------------------------------------
            // Workaround for TFS Bug # 752845, refer bug description for more information
            string line = string.Format(CultureInfo.CurrentCulture, "An exception of type '{0}' occurred and was caught.", base.Exception.GetType().FullName);
            this.Writer.WriteLine(line);

            string separator = new string('-', line.Length);

            this.Writer.WriteLine(separator);
        }

        /// <summary>
        /// Writes and formats the exception and all nested inner exceptions to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="exceptionToFormat">The exception to format.</param>
        /// <param name="outerException">The outer exception. This 
        /// value will be null when writing the outer-most exception.</param>
        protected override void WriteException(Exception exceptionToFormat, Exception outerException)
        {
            if (outerException != null)
            {
                this.innerDepth++;
                this.Indent();
                string temp = ExceptionFormatter.InnerException;
                string separator = new string('-', temp.Length);
                this.Writer.WriteLine(temp);
                this.Indent();
                this.Writer.WriteLine(separator);

                base.WriteException(exceptionToFormat, outerException);
                this.innerDepth--;
            }
            else
            {
                base.WriteException(exceptionToFormat, outerException);
            }

            if (FlattenAggregateException == true && exceptionToFormat is AggregateException)
            {
                ReadOnlyCollection<Exception> exceptions = ((AggregateException)exceptionToFormat).InnerExceptions;
                this.Writer.WriteLine("--output InnerExceptions of AggregateException, total count:{0}", exceptions.Count);
                int idx = 0;
                foreach (var e in exceptions)
                {
                    idx++;
                    this.Writer.WriteLine("-- {0} of {1} of InnerExceptions:", idx, exceptions.Count);
                    base.WriteException(e, null);
                }
            }
        }

        /// <summary>
        /// Writes the current date and time to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="datetime">The current time.</param>
        protected override void WriteDateTime(DateTime datetime)
        {
            this.Writer.WriteLine(datetime.ToUniversalTime().ToString("G", DateTimeFormatInfo.InvariantInfo));
        }

        /// <summary>
        /// Writes the value of the <see cref="Type.AssemblyQualifiedName"/>
        /// property for the specified exception type to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="exceptionType">The <see cref="Type"/> of the exception.</param>
        protected override void WriteExceptionType(Type exceptionType)
        {
            if (exceptionType == null)
            {
                throw new ArgumentException("Exception type cannot be null");
            }

            IndentAndWriteLine(ExceptionFormatter.TypeString, exceptionType.AssemblyQualifiedName);
        }

        /// <summary>
        /// Writes the value of the <see cref="Exception.Message"/>
        /// property to the underyling <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="message">The message to write.</param>
        protected override void WriteMessage(string message)
        {
            IndentAndWriteLine(ExceptionFormatter.Message, message);
        }

        /// <summary>
        /// Writes the value of the specified source taken
        /// from the value of the <see cref="Exception.Source"/>
        /// property to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="source">The source of the exception.</param>
        protected override void WriteSource(string source)
        {
            IndentAndWriteLine(ExceptionFormatter.Source, source);
        }

        /// <summary>
        /// Writes the value of the specified help link taken
        /// from the value of the <see cref="Exception.HelpLink"/>
        /// property to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="helpLink">The exception's help link.</param>
        protected override void WriteHelpLink(string helpLink)
        {
            IndentAndWriteLine(ExceptionFormatter.HelpLink, helpLink);
        }

        /// <summary>
        /// Writes the name and value of the specified property to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="propertyInfo">The reflected <see cref="PropertyInfo"/> object.</param>
        /// <param name="value">The value of the <see cref="PropertyInfo"/> object.</param>
        protected override void WritePropertyInfo(PropertyInfo propertyInfo, object value)
        {
            if (propertyInfo == null)
            {
                throw new ArgumentException("Property info cannot be null");
            }

            this.Indent();
            this.Writer.Write(propertyInfo.Name);
            this.Writer.Write(" : ");
            this.Writer.WriteLine(value);
        }

        /// <summary>
        /// Writes the name and value of the specified field to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="fieldInfo">The reflected <see cref="FieldInfo"/> object.</param>
        /// <param name="value">The value of the <see cref="FieldInfo"/> object.</param>
        protected override void WriteFieldInfo(FieldInfo fieldInfo, object value)
        {
            if (fieldInfo == null)
            {
                throw new ArgumentException("Field info cannot be null");
            }

            this.Indent();
            this.Writer.Write(fieldInfo.Name);
            this.Writer.Write(" : ");
            this.Writer.WriteLine(value);
        }

        /// <summary>
        /// Writes the value of the <see cref="System.Exception.StackTrace"/> property to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="stackTrace">The stack trace of the exception.</param>
        /// <remarks>
        /// If there is no stack trace available, an appropriate message will be displayed.
        /// </remarks>
        protected override void WriteStackTrace(string stackTrace)
        {
            this.Indent();
            this.Writer.Write(ExceptionFormatter.StackTrace);
            this.Writer.Write(" : ");
            if (stackTrace == null || stackTrace.Length == 0)
            {
                this.Writer.WriteLine(ExceptionFormatter.StackTraceUnavailable);
            }
            else
            {
                // The stack trace has all '\n's prepended with a number
                // of tabs equal to the InnerDepth property in order
                // to make the formatting pretty.
                string indentation = new String('\t', this.innerDepth);
                string indentedStackTrace = stackTrace.Replace("\n", "\n" + indentation);

                this.Writer.WriteLine(indentedStackTrace);
                this.Writer.WriteLine();
            }
        }

        /// <summary>
        /// Writes the additional properties to the <see cref="TextWriter"/>.
        /// </summary>
        /// <param name="additionalInformation">Additional information to be included with the exception report</param>
        protected override void WriteAdditionalInfo(NameValueCollection additionalInformation)
        {
            if (additionalInformation == null)
            {
                throw new ArgumentException("Additional information cannot be null");
            }

            this.Writer.WriteLine(ExceptionFormatter.AdditionalInfoConst);
            this.Writer.WriteLine();

            foreach (string name in additionalInformation.AllKeys)
            {
                this.Writer.Write(name);
                this.Writer.Write(" : ");
                this.Writer.Write(additionalInformation[name]);
                this.Writer.Write("\n");
            }
        }

        /// <summary>
        /// Indents the <see cref="TextWriter"/>.
        /// </summary>
        private void Indent()
        {
            for (int i = 0; i < innerDepth; i++)
            {
                this.Writer.Write("\t");
            }
        }

        private void IndentAndWriteLine(string format, params object[] arg)
        {
            this.Indent();
            this.Writer.WriteLine(format, arg);
        }
    }

    /// <summary>
    /// Represents the base class from which all implementations of exception formatters must derive. The formatter provides functionality for formatting <see cref="Exception"/> objects.
    /// </summary>	
    public abstract class ExceptionFormatter
    {
        // <autogenerated />
        public const String AdditionalInfoConst = "Additional Info:";
        public const String ExceptionWasCaught = "An exception of type '{0}' occurred and was caught.";
        public const String FieldAccessFailed = "Access failed";
        public const String HelpLink = "Help link : {0}";
        public const String InnerException = "Inner Exception";
        public const String Message = "Message : {0}";
        public const String PermissionDenied = "Permission Denied";
        public const String PropertyAccessFailed = "Access failed";
        public const String Source = "Source : {0}";
        public const String StackTrace = "Stack Trace";
        public const String StackTraceUnavailable = "The stack trace is unavailable.";
        public const String TypeString = "Type : {0}";
        public const String UndefinedValue = "<undefined value>";

        private static readonly ArrayList IgnoredProperties = new ArrayList(
            new String[] { "Source", "Message", "HelpLink", "InnerException", "StackTrace" });

        private readonly Guid handlingInstanceId;
        private readonly Exception exception;
        private NameValueCollection additionalInfo;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExceptionFormatter"/> class with an <see cref="Exception"/> to format.
        /// </summary>
        /// <param name="exception">The <see cref="Exception"/> object to format.</param>
        /// <param name="handlingInstanceId">The id of the handling chain.</param>
        protected ExceptionFormatter(Exception exception, Guid handlingInstanceId)
        {
            if (exception == null) throw new ArgumentNullException("exception");

            this.exception = exception;
            this.handlingInstanceId = handlingInstanceId;
        }

        /// <summary>
        /// Gets the <see cref="Exception"/> to format.
        /// </summary>
        /// <value>
        /// The <see cref="Exception"/> to format.
        /// </value>
        public Exception Exception
        {
            get { return this.exception; }
        }

        /// <summary>
        /// Gets the id of the handling chain requesting a formatting.
        /// </summary>
        /// <value>
        /// The id of the handling chain requesting a formatting, or <see cref="Guid.Empty"/> if no such id is available.
        /// </value>
        public Guid HandlingInstanceId
        {
            get { return this.handlingInstanceId; }
        }

        /// <summary>
        /// Gets additional information related to the <see cref="Exception"/> but not
        /// stored in the exception (eg: the time in which the <see cref="Exception"/> was 
        /// thrown).
        /// </summary>
        /// <value>
        /// Additional information related to the <see cref="Exception"/> but not
        /// stored in the exception (for example, the time when the <see cref="Exception"/> was 
        /// thrown).
        /// </value>
        public NameValueCollection AdditionalInfo
        {
            get
            {
                if (this.additionalInfo == null)
                {
                    this.additionalInfo = new NameValueCollection();
                    this.additionalInfo.Add("MachineName", GetMachineName());
                    this.additionalInfo.Add("TimeStamp", DateTime.UtcNow.ToString(CultureInfo.CurrentCulture));
                    this.additionalInfo.Add("FullName", Assembly.GetExecutingAssembly().FullName);
                    this.additionalInfo.Add("AppDomainName", AppDomain.CurrentDomain.FriendlyName);
                    this.additionalInfo.Add("ThreadIdentity", Thread.CurrentPrincipal.Identity.Name);
                    this.additionalInfo.Add("WindowsIdentity", GetWindowsIdentity());
                }

                return this.additionalInfo;
            }
        }

        public bool FlattenAggregateException
        {
            get;
            set;
        }
        /// <summary>
        /// Formats the <see cref="Exception"/> into the underlying stream.
        /// </summary>
        public virtual void Format()
        {
            WriteDescription();
            WriteDateTime(DateTime.UtcNow);
            WriteException(this.exception, null);
        }

        /// <summary>
        /// Formats the exception and all nested inner exceptions.
        /// </summary>
        /// <param name="exceptionToFormat">The exception to format.</param>
        /// <param name="outerException">The outer exception. This 
        /// value will be null when writing the outer-most exception.</param>
        /// <remarks>
        /// <para>This method calls itself recursively until it reaches
        /// an exception that does not have an inner exception.</para>
        /// <para>
        /// This is a template method which calls the following
        /// methods in order
        /// <list type="number">
        /// <item>
        /// <description><see cref="WriteExceptionType"/></description>
        /// </item>
        /// <item>
        /// <description><see cref="WriteMessage"/></description>
        /// </item>
        /// <item>
        /// <description><see cref="WriteSource"/></description>
        /// </item>
        /// <item>
        /// <description><see cref="WriteHelpLink"/></description>
        /// </item>
        /// <item>
        /// <description><see cref="WriteReflectionInfo"/></description>
        /// </item>
        /// <item>
        /// <description><see cref="WriteStackTrace"/></description>
        /// </item>
        /// <item>
        /// <description>If the specified exception has an inner exception
        /// then it makes a recursive call. <see cref="WriteException"/></description>
        /// </item>
        /// </list>
        /// </para>
        /// </remarks>
        protected virtual void WriteException(Exception exceptionToFormat, Exception outerException)
        {
            if (exceptionToFormat == null) throw new ArgumentNullException("exceptionToFormat");

            this.WriteExceptionType(exceptionToFormat.GetType());
            this.WriteMessage(exceptionToFormat.Message);
            this.WriteSource(exceptionToFormat.Source);
            this.WriteHelpLink(exceptionToFormat.HelpLink);
            this.WriteReflectionInfo(exceptionToFormat);
            this.WriteStackTrace(exceptionToFormat.StackTrace);

            // We only want additional information on the top most exception
            if (outerException == null)
            {
                this.WriteAdditionalInfo(this.AdditionalInfo);
            }

            Exception inner = exceptionToFormat.InnerException;

            if (inner != null)
            {
                // recursive call
                this.WriteException(inner, exceptionToFormat);
            }

        }

        /// <summary>
        /// Formats an <see cref="Exception"/> using reflection to get the information.
        /// </summary>
        /// <param name="exceptionToFormat">
        /// The <see cref="Exception"/> to be formatted.
        /// </param>
        /// <remarks>
        /// <para>This method reflects over the public, instance properties 
        /// and public, instance fields
        /// of the specified exception and prints them to the formatter.  
        /// Certain property names are ignored
        /// because they are handled explicitly in other places.</para>
        /// </remarks>
        protected void WriteReflectionInfo(Exception exceptionToFormat)
        {
            if (exceptionToFormat == null) throw new ArgumentNullException("exceptionToFormat");

            Type type = exceptionToFormat.GetType();
            PropertyInfo[] properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);
            FieldInfo[] fields = type.GetFields(BindingFlags.Instance | BindingFlags.Public);
            object value;

            foreach (PropertyInfo property in properties)
            {
                if (property.CanRead && IgnoredProperties.IndexOf(property.Name) == -1 && property.GetIndexParameters().Length == 0)
                {
                    try
                    {
                        value = property.GetValue(exceptionToFormat, null);
                    }
                    catch (TargetInvocationException)
                    {
                        value = ExceptionFormatter.PropertyAccessFailed;
                    }
                    WritePropertyInfo(property, value);
                }
            }

            foreach (FieldInfo field in fields)
            {
                try
                {
                    value = field.GetValue(exceptionToFormat);
                }
                catch (TargetInvocationException)
                {
                    value = ExceptionFormatter.FieldAccessFailed;
                }
                WriteFieldInfo(field, value);
            }
        }

        /// <summary>
        /// When overridden by a class, writes a description of the caught exception.
        /// </summary>
        protected abstract void WriteDescription();

        /// <summary>
        /// When overridden by a class, writes the current time.
        /// </summary>
        /// <param name="dateTime">The current time.</param>
        protected abstract void WriteDateTime(DateTime dateTime);

        /// <summary>
        /// When overridden by a class, writes the <see cref="Type"/> of the current exception.
        /// </summary>
        /// <param name="exceptionType">The <see cref="Type"/> of the exception.</param>
        protected abstract void WriteExceptionType(Type exceptionType);

        /// <summary>
        /// When overridden by a class, writes the <see cref="System.Exception.Message"/>.
        /// </summary>
        /// <param name="message">The message to write.</param>
        protected abstract void WriteMessage(string message);

        /// <summary>
        /// When overridden by a class, writes the value of the <see cref="System.Exception.Source"/> property.
        /// </summary>
        /// <param name="source">The source of the exception.</param>
        protected abstract void WriteSource(string source);

        /// <summary>
        /// When overridden by a class, writes the value of the <see cref="System.Exception.HelpLink"/> property.
        /// </summary>
        /// <param name="helpLink">The help link for the exception.</param>
        protected abstract void WriteHelpLink(string helpLink);

        /// <summary>
        /// When overridden by a class, writes the value of the <see cref="System.Exception.StackTrace"/> property.
        /// </summary>
        /// <param name="stackTrace">The stack trace of the exception.</param>
        protected abstract void WriteStackTrace(string stackTrace);

        /// <summary>
        /// When overridden by a class, writes the value of a <see cref="PropertyInfo"/> object.
        /// </summary>
        /// <param name="propertyInfo">The reflected <see cref="PropertyInfo"/> object.</param>
        /// <param name="value">The value of the <see cref="PropertyInfo"/> object.</param>
        protected abstract void WritePropertyInfo(PropertyInfo propertyInfo, object value);

        /// <summary>
        /// When overridden by a class, writes the value of a <see cref="FieldInfo"/> object.
        /// </summary>
        /// <param name="fieldInfo">The reflected <see cref="FieldInfo"/> object.</param>
        /// <param name="value">The value of the <see cref="FieldInfo"/> object.</param>
        protected abstract void WriteFieldInfo(FieldInfo fieldInfo, object value);

        /// <summary>
        /// When overridden by a class, writes additional properties if available.
        /// </summary>
        /// <param name="additionalInformation">Additional information to be included with the exception report</param>
        protected abstract void WriteAdditionalInfo(NameValueCollection additionalInformation);

        private static string GetMachineName()
        {
            string machineName = String.Empty;
            try
            {
                machineName = Environment.MachineName;
            }
            catch (SecurityException)
            {
                machineName = ExceptionFormatter.PermissionDenied;
            }

            return machineName;
        }

        private static string GetWindowsIdentity()
        {
            string windowsIdentity = String.Empty;
            try
            {
                windowsIdentity = WindowsIdentity.GetCurrent().Name;
            }
            catch (SecurityException)
            {
                windowsIdentity = ExceptionFormatter.PermissionDenied;
            }

            return windowsIdentity;
        }
    }
}
