using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

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
