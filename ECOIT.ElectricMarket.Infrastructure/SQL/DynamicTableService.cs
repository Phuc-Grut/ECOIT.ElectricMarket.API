using ECOIT.ElectricMarket.Aplication.Interface;
using Microsoft.Data.SqlClient;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Dapper;
using OfficeOpenXml;
using System.Data;

namespace ECOIT.ElectricMarket.Infrastructure.SQL
{
    public class DynamicTableService : IDynamicTableService
    {
        private readonly string _connectionString;
        public DynamicTableService(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }
        public async Task CreateTableAsync(string tableName, List<string> columnNames)
        {
            Console.WriteLine(tableName);
            var safeTableName = $"{Regex.Replace(tableName, @"\W+", "")}";
            var columnsSql = columnNames
                .Select(col => $"[{Regex.Replace(col, @"\W+", "")}] NVARCHAR(MAX)");

            var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnsSql)})";

            using var conn = new SqlConnection(_connectionString);
            await conn.ExecuteAsync(createSql);
        }

        public async Task InsertDataAsync(string tableName, List<string> columnNames, List<List<string>> rows)
        {
            var safeTableName = $"{Regex.Replace(tableName, @"\W+", "")}";

            // Tạo DataTable
            var table = new DataTable();
            foreach (var col in columnNames)
            {
                table.Columns.Add(Regex.Replace(col, @"\W+", ""), typeof(string));
            }

            // Thêm dữ liệu vào DataTable
            foreach (var row in rows)
            {
                table.Rows.Add(row.ToArray());
            }

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = safeTableName
            };

            foreach (var col in columnNames)
            {
                var safeCol = Regex.Replace(col, @"\W+", "");
                bulkCopy.ColumnMappings.Add(safeCol, safeCol);
            }

            await bulkCopy.WriteToServerAsync(table);
        }

        //public async Task InsertDataAsync(string tableName, List<List<string>> rows)
        //{
        //    var safeTableName = $"{Regex.Replace(tableName, @"\W+", "")}";

        //    using var conn = new SqlConnection(_connectionString);

        //    foreach (var row in rows)
        //    {
        //        var values = row.Select(v => $"N'{v.Replace("'", "''")}'");
        //        var sql = $"INSERT INTO [{safeTableName}] VALUES ({string.Join(",", values)})";
        //        await conn.ExecuteAsync(sql);
        //    }
        //}

    }
}
