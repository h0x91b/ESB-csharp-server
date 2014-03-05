using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using log4net;

[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Log4Net.config", Watch = true)]

namespace ESBServer
{
    class Program
    {
        static void Main(string[] args)
        {
            new Proxy();

            while (true)
            {
                Thread.Sleep(1000);
            }
        }
    }
}
