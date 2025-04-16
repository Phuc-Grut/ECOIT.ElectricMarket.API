using ECOIT.ElectricMarket.Aplication.Interface;
using Microsoft.Data.SqlClient;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Dapper;
using OfficeOpenXml;
using System.Data;
using System.Globalization;
using System.Text;

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
            var safeTableName = Regex.Replace(tableName, @"\W+", "");

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var checkCmd = new SqlCommand(@"
            IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();

            if (exists == 1)
                throw new Exception($" Bảng '{safeTableName}' đã tồn tại, không thể thêm mới.");

            var columnsSql = columnNames
                .Select(col => $"[{Regex.Replace(col, @"\W+", "")}] NVARCHAR(MAX)");

            var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnsSql)})";

            await conn.ExecuteAsync(createSql);
        }

        public async Task<List<Dictionary<string, object>>> GetTableDataAsync(string tableName)
        {
            var safeTable = new string(tableName.Where(char.IsLetterOrDigit).ToArray());

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new SqlCommand($"SELECT * FROM [{safeTable}]", conn);
            var adapter = new SqlDataAdapter(cmd);
            var table = new DataTable();
            adapter.Fill(table);

            var result = new List<Dictionary<string, object>>();
            foreach (DataRow row in table.Rows)
            {
                var dict = new Dictionary<string, object>();
                foreach (DataColumn col in table.Columns)
                {
                    dict[col.ColumnName] = row[col] is DBNull ? null : row[col];
                }
                result.Add(dict);
            }

            return result;
        }

        public async Task<List<string>> GetTableNamesAsync()
        {
            var tables = new List<string>();

            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var cmd = new SqlCommand(@"
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        ", conn);

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(reader.GetString(0));
            }

            return tables;
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
