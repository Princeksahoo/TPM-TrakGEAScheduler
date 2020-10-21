using System;
using System.Collections.Generic;
using System.ServiceProcess;
using System.Text;

namespace GEAScheduler
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {

#if (!DEBUG)
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[] { new TPMTrakScheduleGEA() };
            ServiceBase.Run(ServicesToRun);
#else
            TPMTrakScheduleGEA service = new TPMTrakScheduleGEA();
            service.StartDebug();
            System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
#endif

        }
    }
}
