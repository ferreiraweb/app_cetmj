using System;
using System.Data.SqlClient;


string Server = "10.39.64.50";
string User = "sisproj";
string password = "s15pr0jc3tr10";

string connectionString = $"Server={Server};User={User};password={password}";
string sql = "";
SqlConnection connection = new SqlConnection(connectionString);

SqlCommand cmdSelect = new SqlCommand();
SqlCommand cmdIntervalo = new SqlCommand();
SqlCommand cmdMaxId = new SqlCommand();


int batchSize = 500;
string tableSource = "DBOCR_2023.dbo.OCR_082023";
int maxId = 0;
string msg = "";
int intervaloInicio = 0;
int intervaloTermino = 0;
List<string?> placasSourceList = new List<string?>();
List<string?> placasNewList = new List<string?>();
int count = 1;


try {

    connection.Open();



    cmdIntervalo.CommandText = $@"
                                    SELECT
                                (SELECT intervalo_inicio 
                                FROM DWOCR.dbo.temp_cetmj WITH(NOLOCK) 
                                WHERE id = 1) intervalo_inicio,
                                (SELECT intervalo_termino 
                                FROM DWOCR.dbo.temp_cetmj WITH(NOLOCK) 
                                WHERE id = 1) intervalo_termino,
                                (
                                    SELECT MAX(Id) FROM DWOCR.dbo.temp_cetmj WITH(NOLOCK) 
                                ) count_tb,
                                (
                                    SELECT MAX(Id) FROM {tableSource} WITH(NOLOCK)
                                ) max_id ";

    cmdIntervalo.Connection = connection;


    using (SqlDataReader reader = cmdIntervalo.ExecuteReader())  {
                        while (reader.Read())
                        {
                            intervaloInicio = Convert.ToInt32(reader["intervalo_inicio"]);
                            intervaloTermino = Convert.ToInt32(reader["intervalo_termino"]);
                            count = Convert.ToInt32(reader["count_tb"]);
                            maxId = Convert.ToInt32(reader["max_id"]);
                        }
        }




   while(intervaloInicio < maxId) {

    placasSourceList.Clear();
    placasNewList.Clear();

    
    cmdSelect.CommandText = $@"
                                SELECT DISTINCT .Placa
                                FROM DBOCR_2023.dbo.OCR_082023 o WITH(NOLOCK) 
                                WHERE Placa IS NOT NULL AND o.Id > {intervaloInicio} AND o.Id < {intervaloTermino}
                                AND o.VeiculoId IS NULL
    ";
    
    cmdSelect.Connection = connection;

     using (SqlDataReader reader = cmdSelect.ExecuteReader())  {
                    while (reader.Read())
                    {
                        placasSourceList.Add(reader["Placa"].ToString());
                    }
    }

   
   } // while



}  
catch(Exception ex) {
    Console.WriteLine(ex.Message);
}

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");
