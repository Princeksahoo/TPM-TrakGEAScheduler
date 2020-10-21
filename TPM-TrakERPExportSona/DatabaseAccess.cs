using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Data.SqlClient;
using System.Data;
using System.Reflection;
using System.Threading;
using System.Configuration;
using OfficeOpenXml;
using System.Xml.Linq;
using System.Linq;

namespace GEAScheduler
{
	public static class DatabaseAccess
	{
        internal static void UpdateStatusForScheduledMachine(string machine)
        {
            SqlConnection conn = ConnectionManager.GetConnection();
            SqlCommand cmd = null;
            string query = @"update ScheduleCalculateMethod_GEA set status=1,ServiceUpdatedTS=GetDate() where Machineid=@machine and status=0";
            try
            {
                cmd = new SqlCommand(query, conn);
                cmd.Parameters.AddWithValue("@machine", machine);
                cmd.ExecuteNonQuery();
            }
            catch(Exception ex)
            {
                Logger.WriteDebugLog(ex.Message);
            }
            finally
            {
                if (conn != null) conn.Close();
            }
        }

        internal static string GetDefaultCalculationMethod()
        {
            string defaultCalcMethod = string.Empty;
            SqlConnection conn = ConnectionManager.GetConnection();
            SqlCommand cmd = null;
            SqlDataReader reader = null;
            string query = @"select ValueInText from ShopDefaults where Parameter='Scheduler_CalculatePlan_GEA'";
            try
            {
                cmd = new SqlCommand(query, conn);
                reader = cmd.ExecuteReader();
                if (reader.Read())
                {
                    defaultCalcMethod = reader["ValueInText"].ToString();
                }
            }
            catch(Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }
            finally
            {
                if (reader != null) reader.Close();
                if (conn != null) conn.Close();
            }
            return defaultCalcMethod;
        }

        internal static List<string> GetTPMTrakEnabledMachines()
        {
            List<string> allMachines = new List<string>();
            SqlConnection conn = ConnectionManager.GetConnection();
            SqlCommand cmd = null;
            SqlDataReader rdr = null;
            string query = @"select machineid from machineinformation where TPMTrakEnabled= 1";
            try
            {
                cmd = new SqlCommand(query, conn);
                rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    allMachines.Add(rdr["machineid"].ToString());
                }
            }
            catch(Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);

            }
            finally
            {
                if (rdr != null) rdr.Close();
                if (conn != null) conn.Close();
            }
            return allMachines;
        }

        internal static bool GenerateSchedulesForGEA(string machine, string CalcMethod, DateTime UserInputDT)
        {
            bool isSuccess = false;
            SqlConnection conn = ConnectionManager.GetConnection();
            SqlCommand cmd = null;
            try
            {
                cmd = new SqlCommand("s_GenerateSchedules_GEA", conn);
                cmd.CommandTimeout = 300;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddWithValue("@Machineid", machine);
                cmd.Parameters.AddWithValue("@CalculatePlan", CalcMethod);
                if (UserInputDT != DateTime.MinValue)
                    cmd.Parameters.AddWithValue("@UserDefinedtime", UserInputDT.ToString("yyyy-MM-dd HH:mm:ss"));
                else
                    cmd.Parameters.AddWithValue("@UserDefinedtime", "");
                        
                int cnt = cmd.ExecuteNonQuery();
                if (cnt >= 0) isSuccess = true;
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
                isSuccess = false;
            }
            finally
            {
                if (conn != null) conn.Close();
            }
            return isSuccess;
        }

        internal static IDictionary<string, string> GetCalculationMethodsWithMachines(out DateTime UserInputDT)
        {
            IDictionary<string, string> dictForMacNCalcMethod = new Dictionary<string, string>();
            SqlConnection conn = ConnectionManager.GetConnection();
            SqlCommand cmd = null;
            SqlDataReader rdr = null;
            UserInputDT = DateTime.MinValue;
            string query = @"select Machineid,CalculatePlan,UserInputDT from ScheduleCalculateMethod_GEA  where status = 0 order by updatedTS desc ";
            try
            {
                cmd = new SqlCommand(query, conn);
                rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    //TODO - check if key exists, if exists ignore it
                    if (!dictForMacNCalcMethod.ContainsKey(rdr["Machineid"].ToString()))
                        dictForMacNCalcMethod.Add(rdr["Machineid"].ToString(), rdr["CalculatePlan"].ToString());
                    if (!DBNull.Value.Equals(rdr["UserInputDT"]))
                    {
                        UserInputDT = Convert.ToDateTime(rdr["UserInputDT"]);                       
                    }
                    
                    //DateTime.TryParse(rdr["UserInputDT"].ToString(), out UserInputDT);
                }
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }
            finally
            {
                if (rdr != null) rdr.Close();
                if (conn != null) conn.Close();
            }
            return dictForMacNCalcMethod;
        }
        internal static bool GenerateRunningSchedulesForGEA()
        {
            bool isSuccess = false;
            SqlConnection conn = ConnectionManager.GetConnection();
            SqlCommand cmd = null;
            try
            {
                cmd = new SqlCommand("s_GenerateRunningSchedules_GEA", conn);
                cmd.CommandType = CommandType.StoredProcedure;
                int cnt=cmd.ExecuteNonQuery();
                if (cnt >= 0)
                    isSuccess = true;
            }
            catch(Exception ex)
            {
                isSuccess = false;
                Logger.WriteErrorLog(ex.Message);
            }
            finally
            {
                if (conn != null) conn.Close();
            }
            return isSuccess;
        }

        public static SqlDataReader GetPreviousShiftEndTime() //Todo
        {

            SqlConnection Con = ConnectionManager.GetConnection();
            //DR0331::Geeta Added from here
            SqlCommand cmd = new SqlCommand("s_GetPreviousShift", Con);  /* returns only current shift Start-End Time */
            cmd.CommandType = CommandType.StoredProcedure;
            SqlDataReader dr = null;
            try
            {
                dr = cmd.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SingleResult);
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }

            return dr;

            //DR0331::Geeta Added till here
            //DR0331::Geeta Commented from here

            //SqlCommand cmd = new SqlCommand("select top 1 Fromtime,Totime,Today, Fromday, ShiftName,ShiftID from shiftdetails where running=1 and convert(datetime,CAST(datePart(hh,ToTime) AS nvarchar(2)) + ':' + CAST(datePart(mi,ToTime) as nvarchar(2))+ ':' + CAST(datePart(ss,ToTime) as nvarchar(2)))<'" + string.Format("{0:HH:mm:ss}", DateTime.Now) + "' order by totime DESC", Con);
            //SqlDataReader dr = null;
            //try
            //{
            //    dr = cmd.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SingleResult);
            //}
            //catch (Exception ex)
            //{
            //    Logger.WriteErrorLog(ex);
            //}
            //return dr;
            //DR0331::Geeta Commented till here
        }

        public static SqlDataReader GetCurrentShiftDetails()
        {

            SqlConnection Con = ConnectionManager.GetConnection();
            SqlCommand cmd = new SqlCommand("s_GetCurrentShiftTime", Con);  /* returns only current shift Start-End Time */
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.Parameters.Add("@StartDate", SqlDbType.DateTime).Value = DateTime.Now;
            cmd.Parameters.Add("@Param", SqlDbType.NVarChar).Value = "";
            SqlDataReader dr = null;
            try
            {
                dr = cmd.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SingleResult);
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }

            return dr;
        }

        public static string GetLogicalDayStart(string LRunDay)
        {
            SqlConnection Con = ConnectionManager.GetConnection();
            SqlCommand cmd = new SqlCommand("SELECT dbo.f_GetLogicalDayStart( '" + string.Format("{0:yyyy-MM-dd HH:mm:ss}", DateTime.Parse(LRunDay).AddSeconds(1)) + "')", Con);

            object SEDate = null;
            try
            {
                SEDate = cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                Logger.WriteErrorLog(ex.Message);
            }
            finally
            {
                if (Con != null)
                {
                    Con.Close();
                }
            }
            if (SEDate == null || Convert.IsDBNull(SEDate))
            {
                return string.Empty;
            }
            return string.Format("{0:yyyy-MM-dd HH:mm:ss}", Convert.ToDateTime(SEDate));
        }  
    }
}
