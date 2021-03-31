using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace SQLConnector
{
    public class Connector
    {
        private readonly string  connectionString;
        private readonly string server;
        private readonly string  database;
        private readonly string  tenantId;
        private readonly string  appId;
        private readonly string  appKey;
        private const string AadInstance = "https://login.microsoftonline.com/";
        private const string SqlResourceUrl = "https://login.microsoftonline.com/";
        private AuthenticationResult  AuthResult { get; set; }

        public Connector(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public Connector(string server, string database, string tenantId, string appId, string appKey)
        {
            this.server = server;
            this.database = database;
            this.tenantId = tenantId;
            this.appId = appId;
            this.appKey = appKey;
            connectionString = $"Data Source={this.server};Initial Catalog={this.database}";
        }

        /// <summary>
        ///  Generate New Token if expired or null
        /// </summary>
        private void RefreshToken()
        {
            try
            {
                var authContext = new AuthenticationContext(AadInstance + tenantId);
                ClientCredential clientCredential = new ClientCredential(appId, appKey);
                AuthResult = authContext.AcquireTokenAsync(SqlResourceUrl, clientCredential).Result;   
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Establish Connection between client and SQL Server
        /// </summary>
        /// <returns></returns>
        private SqlConnection GetSqlConnection()
        {
            SqlConnection sqlConnection = null;
            try
            {
                //--- can be used for more instances
                sqlConnection = new SqlConnection(connectionString);
                if(!string.IsNullOrEmpty(appId) && !string.IsNullOrEmpty(appKey))
                {
                    //--- check for token validity 
                    if( AuthResult == null || ( AuthResult != null &&  AuthResult.ExpiresOn > DateTime.UtcNow))
                    {
                        RefreshToken();
                    }
                    sqlConnection.AccessToken =  AuthResult.AccessToken;
                }
            }
            catch(Exception ex)
            {
                throw ex;
            }
            return sqlConnection;
        }

        /// <summary>
        /// Get single Data Table from SQL
        /// </summary>
        /// <param name="storedProcedure">Stored Procedure Name</param>
        /// <param name="sqlParameters">List of SQL Parameter</param>
        /// <returns></returns>
        public DataTable GetDataTable(string storedProcedure, List<SqlParameter> sqlParameters = null)
        {
            DataTable dataTable = new DataTable();
            try
            {
                DataSet dataSet = GetDataSet(storedProcedure, sqlParameters);
                if(dataSet != null && dataSet.Tables.Count > 0)
                {
                    dataTable = dataSet.Tables[0];
                }
            }
            catch (System.Exception)
            {
                
                throw;
            }
            return dataTable;
        }

        /// <summary>
        /// Get Multiple Data Tables from SQL
        /// </summary>
        /// <param name="storedProcedure">stored Procedure Name</param>
        /// <param name="sqlParameters">List of Sql Parameters</param>
        /// <returns></returns>
        public DataSet GetDataSet(string storedProcedure, List<SqlParameter> sqlParameters = null)
        {
            DataSet dataSet = new DataSet();
            try
            {
                if(!string.IsNullOrEmpty(storedProcedure))
                {
                    using(SqlConnection connection = GetSqlConnection())
                    {
                        SqlCommand command = new SqlCommand(storedProcedure, connection);
                        command.CommandType = CommandType.StoredProcedure;
                        if(sqlParameters != null && sqlParameters.Count > 0)
                        {
                            command.Parameters.AddRange(sqlParameters.ToArray());
                        }

                        SqlDataAdapter adapter = new SqlDataAdapter();
                        adapter.SelectCommand = command;

                        adapter.Fill(dataSet);
                    }
                }
            }
            catch (System.Exception)
            {
                
                throw;
            }
            return dataSet;
        }

        /// <summary>
        /// Insert / Update Sql Data
        /// </summary>
        /// <param name="storedProcedure">Stored Procedure Name</param>
        /// <param name="sqlParameters">List of Sql Parameters</param>
        /// <returns></returns>
        public int ModifyData(string storedProcedure, List<SqlParameter> sqlParameters = null)
        {
            int result = -1;
            try
            {
                if(!string.IsNullOrEmpty(storedProcedure))
                {
                    using(SqlConnection connection = GetSqlConnection())
                    {
                        SqlCommand command = new SqlCommand(storedProcedure, connection);
                        command.CommandType = CommandType.StoredProcedure;
                        if(sqlParameters != null && sqlParameters.Count > 0)
                        {
                            command.Parameters.AddRange(sqlParameters.ToArray());
                        }
                        connection.Open();
                        result = command.ExecuteNonQuery();
                    }
                }
            }
            catch (System.Exception)
            {
                
                throw;
            }
            return result;
        }

        
    }
}
