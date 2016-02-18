// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the Apache License, Version 2.0.  See License.txt in the project root for license information.

namespace KafkaNET.Library.Examples
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    internal class KafkaNETExampleSubArguments
    {
        internal Dictionary<string, string> ArgumentsOptions { get; private set; }
        ////[('z', "zookeeper", Required = true, HelpText = "Zookeeper, example:  machine1:2181 ")]
        internal string Zookeeper;
        internal Dictionary<string, string> RegisteredQuickName = new Dictionary<string, string>();

        internal KafkaNETExampleSubArguments(){ }

        internal string GetArgDict()
        {
            StringBuilder sb = new StringBuilder();
            foreach (var k in this.ArgumentsOptions.Keys.OrderBy(r => r))
            {
                sb.AppendFormat("{0}   {1} \r\n", k, this.ArgumentsOptions[k]);
            }

            return sb.ToString();
        }

        internal virtual void Parse(string[] args)
        {
            throw new NotImplementedException();
        }

        internal virtual string GetUsage(bool simple)
        {
            throw new NotImplementedException();
        }

        protected void CheckZookeeperPort()
        {
            if (string.IsNullOrEmpty(this.Zookeeper))
                this.Zookeeper = "127.0.0.1";
            if (!this.Zookeeper.Contains(":"))
                this.Zookeeper = this.Zookeeper + ":2181";
        }

        protected bool GetInt(string simple, string complex, ref int output)
        {
            CheckQuickName(simple, complex);
            if (!string.IsNullOrEmpty(simple) && this.ArgumentsOptions.ContainsKey(simple.ToLowerInvariant()))
            {
                if (Int32.TryParse(this.ArgumentsOptions[simple.ToLowerInvariant()], out output))
                    return true;
                else
                    throw new ArgumentException(string.Format("Can't parse to int32. Key:{0} Value:{1}", simple, this.ArgumentsOptions[simple.ToLowerInvariant()]));
            }
            else if (!string.IsNullOrEmpty(complex) && this.ArgumentsOptions.ContainsKey(complex.ToLowerInvariant()))
            {

                if (Int32.TryParse(this.ArgumentsOptions[complex.ToLowerInvariant()], out output))
                    return true;
                else
                    throw new ArgumentException(string.Format("Can't parse to int32. Key:{0} Value:{1}", complex, this.ArgumentsOptions[complex.ToLowerInvariant()]));
            }
            else
                return false;
        }

        protected bool GetString(string simple, string complex, ref string output)
        {
            CheckQuickName(simple, complex);
            if (!string.IsNullOrEmpty(simple) && this.ArgumentsOptions.ContainsKey(simple.ToLowerInvariant()))
            {
                output = (this.ArgumentsOptions[simple.ToLowerInvariant()]);
                return true;
            }
            else if (!string.IsNullOrEmpty(complex) && this.ArgumentsOptions.ContainsKey(complex.ToLowerInvariant()))
            {
                output = (this.ArgumentsOptions[complex.ToLowerInvariant()]);
                return true;
            }
            else
                return false;
        }

        protected bool GetBool(string simple, string complex, ref bool output)
        {
            CheckQuickName(simple, complex);
            if (!string.IsNullOrEmpty(simple) && this.ArgumentsOptions.ContainsKey(simple.ToLowerInvariant()))
            {
                if (this.ArgumentsOptions[simple.ToLowerInvariant()] == "1")
                    output = true;
                else
                {
                    if (Boolean.TryParse(this.ArgumentsOptions[simple.ToLowerInvariant()], out output))
                        return true;
                    else
                        throw new ArgumentException(string.Format("Can't parse to Boolean. Key:{0} Value:{1}", simple, this.ArgumentsOptions[simple.ToLowerInvariant()]));
                }
                return true;
            }
            else if (!string.IsNullOrEmpty(complex) && this.ArgumentsOptions.ContainsKey(complex.ToLowerInvariant()))
            {
                if (this.ArgumentsOptions[complex.ToLowerInvariant()] == "1")
                    output = true;
                else
                {
                    if (Boolean.TryParse(this.ArgumentsOptions[complex.ToLowerInvariant()], out output))
                        return true;
                    else
                        throw new ArgumentException(string.Format("Can't parse to Boolean. Key:{0} Value:{1}", complex, this.ArgumentsOptions[complex.ToLowerInvariant()]));
                }
                return true;
            }
            else
                return false;
        }

        protected bool GetShort(string simple, string complex, ref short output)
        {
            CheckQuickName(simple, complex);
            if (!string.IsNullOrEmpty(simple) && this.ArgumentsOptions.ContainsKey(simple.ToLowerInvariant()))
            {
                if (Int16.TryParse(this.ArgumentsOptions[simple.ToLowerInvariant()], out output))
                    return true;
                else
                    throw new ArgumentException(string.Format("Can't parse to Int16. Key:{0} Value:{1}", simple, this.ArgumentsOptions[simple.ToLowerInvariant()]));
            }
            else if (!string.IsNullOrEmpty(complex) && this.ArgumentsOptions.ContainsKey(complex.ToLowerInvariant()))
            {
                if (Int16.TryParse(this.ArgumentsOptions[complex.ToLowerInvariant()], out output))
                    return true;
                else
                    throw new ArgumentException(string.Format("Can't parse to Int16. Key:{0} Value:{1}", complex, this.ArgumentsOptions[complex.ToLowerInvariant()]));
            }
            else
                return false;
        }

        private void CheckQuickName(string simple, string complex)
        {
            if(!string.IsNullOrEmpty(simple))
            {
                if (RegisteredQuickName.ContainsKey(simple.ToLowerInvariant()))
                    throw new ArgumentException(string.Format("The argument key {0} already used for {1}. Now try use for {2}", simple, RegisteredQuickName[simple], complex));
                this.RegisteredQuickName.Add(simple.ToLowerInvariant(), complex);
            }
        }

        protected void BuildDictionary(string[] args)
        {
            this.ArgumentsOptions = new Dictionary<string, string>();
            for (int j = 1; j < args.Length; j++)
            {
                string arg = args[j].ToLowerInvariant();
                string value = "1";
                int checkIfNumeber= 0;
                if (arg.StartsWith("-") && !Int32.TryParse(arg, out checkIfNumeber))
                {
                    if (j + 1 <= args.Length - 1 &&
                        
                        (  !args[j + 1].ToLowerInvariant().StartsWith("-")
                        || (args[j + 1].ToLowerInvariant().StartsWith("-") && Int32.TryParse(args[j + 1], out checkIfNumeber)))
                        
                        )
                    {
                        value = args[j + 1];
                        j += 1;
                    }
                    this.ArgumentsOptions.Add(arg, value);
                }
            }
        }
    }
}
