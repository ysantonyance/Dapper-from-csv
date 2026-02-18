// dotnet add package Z.Dapper.Plus
// dotnet add package Microsoft.Data.SqlClient
// dotnet add package CsvHelper

using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using System.Formats.Asn1;
using System.Globalization;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Z.Dapper.Plus;

namespace ExcelFile
{
    public class DynamicModelGenerator
    {
        public static void Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.Title = "CSV";

            string connectionString = "Server=localhost;Database=Excel;Trusted_Connection=True;TrustServerCertificate=True;";
            string filePath = "C:/Users/zinov/Downloads/Early_Learning_Feedback_Report_-_Number_of_Domains_Ready.csv"; // Public Use Microdata Area https://en.wikipedia.org/wiki/Public_Use_Microdata_Area

            var records = ReadCsv(filePath);

            var modelType = CreateDynamicModel(filePath);

            var dataList = MapDataToModel(records, modelType);

            string tableName = "DynamicModel";         
            CreateTableInDatabase(tableName, modelType, connectionString); 

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                connection.BulkInsert(dataList); 
                Console.WriteLine("Дані успішно вставлено в таблицю.");
            }
        }

        
        static List<Dictionary<string, string>> ReadCsv(string filePath)
        {
            var records = new List<Dictionary<string, string>>();

            using (var reader = new StreamReader(filePath))
            {
                var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true
                };

                using (var csv = new CsvReader(reader, csvConfig))
                {
                    csv.Read();
                    csv.ReadHeader();

                    if (csv.HeaderRecord.Any(string.IsNullOrWhiteSpace))
                    {
                        throw new Exception("Файл CSV містить порожні заголовки! Перевірте перший рядок.");
                    }

                    while (csv.Read())
                    {
                        var row = new Dictionary<string, string>();
                        foreach (var header in csv.HeaderRecord)
                        {
                            if (!string.IsNullOrWhiteSpace(header)) 
                            {
                                row[header] = csv.GetField(header);
                            }
                        }
                        records.Add(row);
                    }
                }
            }
            return records;
        }
        static Type CreateDynamicModel(string filePath)
        {
            var columns = new List<string>();

            using (var reader = new StreamReader(filePath))
            {
                var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true
                };

                using (var csv = new CsvReader(reader, csvConfig))
                {
                    csv.Read();
                    csv.ReadHeader();
                    columns = csv?.HeaderRecord?.Where(h => !string.IsNullOrWhiteSpace(h)).ToList();
                }
            }

            if (columns?.Count == 0)
            {
                throw new Exception("Файл CSV не містить заголовків!");
            }

            Console.WriteLine("Заголовки CSV: " + string.Join(", ", columns));

            var assemblyName = new AssemblyName("DynamicModelAssembly");
            var assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
            var moduleBuilder = assemblyBuilder.DefineDynamicModule("MainModule");
            var typeBuilder = moduleBuilder.DefineType("DynamicModel", TypeAttributes.Public | TypeAttributes.Class);

            foreach (var column in columns)
            {
                var fieldBuilder = typeBuilder.DefineField("_" + column, typeof(string), FieldAttributes.Private);

                var propertyBuilder = typeBuilder.DefineProperty(column, PropertyAttributes.HasDefault, typeof(string), null);

                var getterMethodBuilder = typeBuilder.DefineMethod("get_" + column, MethodAttributes.Public, typeof(string), Type.EmptyTypes);
                var getterIl = getterMethodBuilder.GetILGenerator();
                getterIl.Emit(OpCodes.Ldarg_0);
                getterIl.Emit(OpCodes.Ldfld, fieldBuilder);
                getterIl.Emit(OpCodes.Ret);

                var setterMethodBuilder = typeBuilder.DefineMethod("set_" + column, MethodAttributes.Public, null, new[] { typeof(string) });
                var setterIl = setterMethodBuilder.GetILGenerator();
                setterIl.Emit(OpCodes.Ldarg_0);
                setterIl.Emit(OpCodes.Ldarg_1);
                setterIl.Emit(OpCodes.Stfld, fieldBuilder);
                setterIl.Emit(OpCodes.Ret);

                propertyBuilder.SetGetMethod(getterMethodBuilder);
                propertyBuilder.SetSetMethod(setterMethodBuilder);
            }

            return typeBuilder.CreateType();
        }

        static List<object> MapDataToModel(List<Dictionary<string, string>> records, Type modelType)
        {
            var dataList = new List<object>();

            foreach (var row in records)
            {
                if (row == null || row.Count == 0) continue;

                var instance = Activator.CreateInstance(modelType);

                foreach (var kvp in row)
                {
                    var property = modelType.GetProperty(kvp.Key);
                    if (property != null)
                    {
                        property.SetValue(instance, kvp.Value);
                    }
                }

                dataList.Add(instance);
            }

            return dataList;
        }
        static void CreateTableInDatabase(string tableName, Type modelType, string connectionString)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var columns = modelType.GetProperties()
                    .Select(prop => $"[{prop.Name}] NVARCHAR(MAX)") 
                    .ToList();

                string createTableQuery = $@"
                    IF OBJECT_ID('{tableName}', 'U') IS NULL
                    CREATE TABLE {tableName} (
                        {string.Join(", ", columns)}
                    )";

                using (var command = new SqlCommand(createTableQuery, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}