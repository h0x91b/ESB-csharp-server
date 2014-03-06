using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using log4net;
using System.ServiceProcess;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using System.IO;
using System.Configuration;

[assembly: log4net.Config.XmlConfigurator(ConfigFile = "Log4Net.config", Watch = true)]

namespace ESBServer
{
    class Program
    {
        [DllImport("kernel32.dll", EntryPoint = "GetStdHandle", SetLastError = true, CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall)]
        private static extern IntPtr GetStdHandle(int nStdHandle);

        [DllImport("kernel32.dll", EntryPoint = "AllocConsole", SetLastError = true, CharSet = CharSet.Auto, CallingConvention = CallingConvention.StdCall)]
        private static extern int AllocConsole();

        private const int STD_OUTPUT_HANDLE = -11;
        private const int MY_CODE_PAGE = 437;

        private static ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType.Name);

        public static bool RunAsService = true;

        static void Main(string[] args)
        {
            RunAsService = bool.Parse(ConfigurationManager.AppSettings["RunAsService"].ToString());

            if (RunAsService)
            {
                log.InfoFormat("Running as Service");

                // set the current directory to the directory of the .exe
                string strEXE = System.Environment.CommandLine;
                strEXE = strEXE.Replace("\"", "");
                strEXE = strEXE.Replace(" ", "");
                string strPath = strEXE.Substring(0, strEXE.LastIndexOf('\\'));

                Directory.SetCurrentDirectory(strPath);

                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[] { new Proxy() };

                ServiceBase.Run(ServicesToRun);
            }
            else
            {
                AllocConsole();
                IntPtr stdHandle = GetStdHandle(STD_OUTPUT_HANDLE);
                SafeFileHandle safeFileHandle = new SafeFileHandle(stdHandle, true);
                FileStream fileStream = new FileStream(safeFileHandle, FileAccess.Write);
                Encoding encoding = System.Text.Encoding.GetEncoding(MY_CODE_PAGE);
                StreamWriter standardOutput = new StreamWriter(fileStream, encoding);
                standardOutput.AutoFlush = true;
                Console.SetOut(standardOutput);

                log.InfoFormat("Running as Command Line");

                new Proxy();
                while (true)
                {
                    Thread.Sleep(1000);
                }
            }
        }
    }
}
