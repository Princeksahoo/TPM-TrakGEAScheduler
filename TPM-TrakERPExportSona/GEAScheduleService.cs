using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Security.Permissions;
using System.ServiceProcess;
using System.Threading;
using System.Timers;

namespace GEAScheduler
{
	partial class TPMTrakScheduleGEA : ServiceBase
	{
		List<Thread> threads = new List<Thread>();
		private readonly object padlock = new object();
		private volatile bool stopping = false;
		int timeDelayForDemand = Convert.ToInt32(ConfigurationManager.AppSettings["IntervalToCallProcOnDemand"]);
		int intervalForProc = Convert.ToInt32(ConfigurationManager.AppSettings["IntervalToCallProc"]);
        int timedelayForShiftEnd = Convert.ToInt32(ConfigurationManager.AppSettings["IntervalToCallProcOnShiftEnd"]);

		public TPMTrakScheduleGEA()
		{
			InitializeComponent();
		}

		[SecurityPermission(SecurityAction.Demand, Flags = SecurityPermissionFlag.ControlAppDomain)]
		protected override void OnStart(string[] args)
		{
			AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

			try
			{
				ThreadStart job = new ThreadStart(GenerateScheduleInIntervals);
				Thread thread = new Thread(job);
				thread.Name = "GEAGenerateSchedulesThread";
				thread.Start();
				threads.Add(thread);
                Logger.WriteDebugLog("GEAGenerateSchedulesThread Service Started Successfully");
			}
			catch (Exception ex)
			{
				Logger.WriteErrorLog(ex.ToString());
			}
			try
			{
				ThreadStart job2 = new ThreadStart(GenerateScheduleOnDemandOnShiftEnd);
				Thread thread2 = new Thread(job2);
				thread2.Name = "GEAGenerateSchedulesOnDemandnShiftEndThread";
				thread2.Start();
				threads.Add(thread2);
                Logger.WriteDebugLog("GEAGenerateSchedulesOnDemandnShiftEndThread Service Started Successfully");
			}
			catch (Exception ex)
			{
				Logger.WriteErrorLog(ex.ToString());
			}
        }

		void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs args)
		{
			Exception e = args.ExceptionObject as Exception;
			Logger.WriteErrorLog("UnhandledException caught : " + e.ToString());
			Logger.WriteErrorLog("Runtime terminating:" + args.IsTerminating);
			Logger.WriteErrorLog(args.ToString());
		}

		protected override void OnStop()
		{
			if (string.IsNullOrEmpty(Thread.CurrentThread.Name))
			{
				Thread.CurrentThread.Name = "ServiceThread";
			}
			stopping = true;
			lock (padlock)
			{
				Monitor.Pulse(padlock);
			}
			Thread.SpinWait(60000 * 10);
			try
			{
				Logger.WriteDebugLog("Service Stop request has come!!! ");
				Logger.WriteDebugLog("Thread count is: " + threads.Count.ToString());
				foreach (Thread thread in threads)
				{
					Logger.WriteDebugLog("Stopping the thread - " + thread.Name);
					thread.Abort();
				}
				threads.Clear();
			}
			catch (Exception ex)
			{
				Logger.WriteErrorLog(ex.Message);
			}
			Logger.WriteDebugLog("Service has stopped.");

		}

		internal void StartDebug()
		{
			OnStart(null);
		}

		private void GenerateScheduleInIntervals()
		{
            while (!stopping)
            {
                try
                {
                    if (DatabaseAccess.GenerateRunningSchedulesForGEA())
                        Logger.WriteDebugLog("Running Schedules Generated for GEA successfully.");
                }
                catch (Exception ex)
                {
                    Logger.WriteErrorLog(ex.Message);
                }
                finally
                {
                    Thread.Sleep(intervalForProc * 1000);
                }
            }
        }

        private void GenerateScheduleOnDemandOnShiftEnd()
		{
			DateTime CurrentShiftEndTime = GetCurrentShiftEndTime().AddMinutes(timedelayForShiftEnd);
			DateTime UserInputDT;
			//todo add min
			while (!stopping)
            {
				try
				{
					#region On Demand
					IDictionary<string, string> dictForMachinesNCalcMethod = DatabaseAccess.GetCalculationMethodsWithMachines(out UserInputDT);
					if (dictForMachinesNCalcMethod != null && dictForMachinesNCalcMethod.Count > 0)
					{
						foreach (KeyValuePair<string, string> item in dictForMachinesNCalcMethod)
						{
							if (DatabaseAccess.GenerateSchedulesForGEA(item.Key, item.Value, UserInputDT))
							{
								Logger.WriteDebugLog("GEA Schedule On Demand Generated successfully for Machine :" + item.Key);
								DatabaseAccess.UpdateStatusForScheduledMachine(item.Key);
							}
						}
						
					}
					else
					{
						Logger.WriteDebugLog("No On Demand Schedules For GEA");
					}
					#endregion

					#region On Shift End
					if (DateTime.Now > CurrentShiftEndTime)
					{
						//Thread.Sleep(timedelayForShiftEnd * 1000);
						Logger.WriteDebugLog("GEA Schudules on Shift End Started");
						var ListOfMachines = DatabaseAccess.GetTPMTrakEnabledMachines();
						string DefaultCalculationMethod = DatabaseAccess.GetDefaultCalculationMethod();
						if (ListOfMachines != null && ListOfMachines.Count > 0)
						{
							foreach (string machine in ListOfMachines)
							{
								if (DatabaseAccess.GenerateSchedulesForGEA(machine, DefaultCalculationMethod,DateTime.MinValue))
									Logger.WriteDebugLog("GEA Schedule Generated Successfully for Machine :" + machine);
							}
						}
						CurrentShiftEndTime = GetCurrentShiftEndTime().AddMinutes(timedelayForShiftEnd);
						//todo -  add min to CurrentShiftEndTime
						Logger.WriteDebugLog("GEA Schudules on Shift End Ended");
					}
					#endregion
				}
				catch (Exception ex)
				{
					Logger.WriteErrorLog(ex.Message);
				}
				finally
				{
					Thread.Sleep(timeDelayForDemand * 1000);
				}
            }
        }

		private DateTime GetPreviousShiftEndTime()
		{
			string EndTime = DateTime.Now.ToString();
			string StartTime = string.Empty;
			SqlDataReader DR = DatabaseAccess.GetPreviousShiftEndTime();
			if (DR.Read())
			{
				//DR0331 :: Geeta added from here
				StartTime = Convert.ToString(DR["Starttime"]);
				EndTime = Convert.ToString(DR["Endtime"]);
			}
			else
			{
				StartTime = string.Empty;
				EndTime = string.Empty;
			}
			if (DR != null)
			{
				DR.Close();
			}
			return DateTime.Parse(EndTime);
		}

		public DateTime GetCurrentShiftEndTime()
		{
			SqlDataReader DR = DatabaseAccess.GetCurrentShiftDetails();
			DateTime EndTime = DateTime.Now;
			if (DR.HasRows)
			{
				DR.Read();
				EndTime = DateTime.Parse(Convert.ToString(DR["Endtime"]));
				if (DR != null)
				{
					DR.Close();
				}
			}
			else
			{
				DateTime logicaldaystart = DateTime.Parse(DatabaseAccess.GetLogicalDayStart(DateTime.Now.ToString("yyyy-MMM-dd hh:mm:ss tt")));
				if (logicaldaystart > DateTime.Now)
				{
					EndTime = logicaldaystart;
				}
				else
				{
					EndTime = logicaldaystart.AddDays(1);

				}
			}
			return EndTime;
		}
	}
}
