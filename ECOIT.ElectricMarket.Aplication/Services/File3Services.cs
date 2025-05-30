﻿using ECOIT.ElectricMarket.Application.Interface;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Text.RegularExpressions;

namespace ECOIT.ElectricMarket.Application.Services
{
    public class File3Services : IFile3Services
    {
        private readonly string _connectionString;

        public File3Services(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("DefaultConnection");
        }
        public async Task Tinh3Gia(string tableName)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var dt = new DataTable();
            using (var adapter = new SqlDataAdapter($"SELECT * FROM [{tableName}]", conn))
            {
                adapter.FillSchema(dt, SchemaType.Source);
                adapter.Fill(dt);
            }

            if (dt.Rows.Count > 0 && dt.Rows[0]["Chukì"].ToString()?.Trim().ToLower() == "ngày")
            {
                dt.Rows[0].Delete();
                dt.AcceptChanges();
            }

            var colsTD = GetCotTongTD(dt);
            var colsCD = GetCotCaoDiem(dt);

            var dtResult = new DataTable();
            dtResult.Columns.Add("Chukì", typeof(string));
            dtResult.Columns.Add("Thứ", typeof(string));
            dtResult.Columns.Add("BT", typeof(string));
            dtResult.Columns.Add("CD", typeof(string));
            dtResult.Columns.Add("TD", typeof(string));

            foreach (DataRow row in dt.Rows)
            {
                string col2 = row["Col2"]?.ToString()?.Trim();
                string chuki = row["Chukì"]?.ToString();

                decimal td = TinhTong(row, colsTD);
                decimal cd = col2?.ToLower() == "sunday" ? 0 : TinhTong(row, colsCD);

                decimal tong = 0;
                if (decimal.TryParse(row["Tổng"]?.ToString(), out var tongFromData))
                    tong = tongFromData;

                decimal bt = tong - td - cd;

                var newRow = dtResult.NewRow();
                newRow["Chukì"] = chuki;
                newRow["Thứ"] = col2;

                var culture = new CultureInfo("en-US");

                newRow["BT"] = bt.ToString("#,##0", culture);
                newRow["CD"] = cd.ToString("#,##0", culture);
                newRow["TD"] = td.ToString("#,##0", culture);

                dtResult.Rows.Add(newRow);
            }

            string newTableName = "3Gia_" + tableName;

            var createSql = @$"
                IF OBJECT_ID('{newTableName}', 'U') IS NOT NULL DROP TABLE [{newTableName}];
                CREATE TABLE [{newTableName}] (
                    [Chukì] NVARCHAR(100),
                    [Thứ] NVARCHAR(20),
                    [BT] NVARCHAR(20),
                    [CD] NVARCHAR(20),
                    [TD] NVARCHAR(20)
                )";

            using (var cmd = new SqlCommand(createSql, conn))
                await cmd.ExecuteNonQueryAsync();

            using (var bulk = new SqlBulkCopy(conn))
            {
                bulk.DestinationTableName = $"[{newTableName}]";
                foreach (DataColumn col in dtResult.Columns)
                    bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                await bulk.WriteToServerAsync(dtResult);
            }
        }

        private decimal TinhTong(DataRow row, List<string> columnNames)
        {
            decimal sum = 0;
            foreach (var col in columnNames)
            {
                if (decimal.TryParse(row[col]?.ToString(), out var val))
                    sum += val;
            }
            return Math.Round(sum, 5);
        }

        private List<string> GetCotTongTD(DataTable table)
        {
            var columns = new List<string>();
            bool inFirst = false, inSecond = false;

            foreach (DataColumn col in table.Columns)
            {
                var name = col.ColumnName.Trim();

                if (name == "0h30") inFirst = true;
                if (name == "4h30") inFirst = false;

                if (name == "22h30") inSecond = true;
                if (name == "24h")
                {
                    columns.Add(name);
                    break;
                }

                if (inFirst || inSecond)
                    columns.Add(name);
            }

            return columns;
        }

        private List<string> GetCotCaoDiem(DataTable table)
        {
            var columns = new List<string>();
            bool inCD1 = false, inCD2 = false;

            foreach (DataColumn col in table.Columns)
            {
                var name = col.ColumnName.Trim();

                if (name == "10h") inCD1 = true;
                if (name == "12h") inCD1 = false;

                if (name == "17h30") inCD2 = true;
                if (name == "20h")
                {
                    columns.Add(name);
                    break;
                }

                if (inCD1 || inCD2)
                    columns.Add(name);
            }

            return columns;
        }

        public async Task CalculateMultiTableFormulaAsync(string outputTable, string outputLabel, string formula, List<string> sourceTables)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var safeTableName = Regex.Replace(outputTable, "\\W+", "");

            var checkCmd = new SqlCommand("IF OBJECT_ID(@tableName, 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);
            checkCmd.Parameters.AddWithValue("@tableName", safeTableName);
            var exists = (int)await checkCmd.ExecuteScalarAsync();
            if (exists == 1) throw new Exception($"Bảng '{safeTableName}' đã tồn tại.");

            var parts = formula.Split('=');
            if (parts.Length != 2) throw new Exception("Công thức không hợp lệ.");
            var right = parts[1].Trim();

            var operands = Regex.Split(right, @"[+\-\*/]").Select(x => x.Trim()).ToList();
            var operators = Regex.Matches(right, @"[+\-\*/]").Cast<Match>().Select(m => m.Value).ToList();

            var tables = new Dictionary<string, DataTable>();
            foreach (var tbl in sourceTables.Distinct())
            {
                var dt = new DataTable();
                using var cmd = new SqlCommand($"SELECT * FROM [{tbl}]", conn);
                using var adapter = new SqlDataAdapter(cmd);
                adapter.Fill(dt);

                if (dt.Rows.Count > 0)
                {
                    var firstRow = dt.Rows[0];
                    if (dt.Columns.Cast<DataColumn>().Any(c => firstRow[c]?.ToString()?.Trim().ToLower() == "ngày"))
                    {
                        dt.Rows[0].Delete();
                        dt.AcceptChanges();
                    }
                }

                tables[tbl] = dt;
            }

            var baseTable = tables[operands[0]];
            var resultTable = baseTable.Clone();
            resultTable.Clear();

            for (int rowIndex = 0; rowIndex < baseTable.Rows.Count; rowIndex++)
            {
                var row = baseTable.Rows[rowIndex];
                var newRow = resultTable.NewRow();

                foreach (DataColumn col in baseTable.Columns)
                {
                    var colName = col.ColumnName;

                    if (colName == "Chukì")
                    {
                        newRow[colName] = NormalizeDate(row[colName]);
                        continue;
                    }
                    if (colName == "Col2")
                    {
                        newRow[colName] = row[colName];
                        continue;
                    }

                    if (colName == "Tổng") continue;

                    decimal result = 0;

                    for (int i = 0; i < operands.Count; i++)
                    {
                        var operandTable = tables[operands[i]];
                        decimal value = 0;

                        if (operandTable.Columns.Contains(colName) && operandTable.Rows.Count > rowIndex)
                        {
                            var obj = operandTable.Rows[rowIndex][colName];
                            decimal.TryParse(obj?.ToString(), out value);
                        }

                        if (i == 0)
                            result = value;
                        else
                        {
                            var op = operators[i - 1];
                            result = op switch
                            {
                                "+" => result + value,
                                "-" => result - value,
                                "*" => result * value,
                                "/" => value != 0 ? result / value : 0,
                                _ => result
                            };
                        }
                    }

                    newRow[colName] = result.ToString("#,##0", CultureInfo.InvariantCulture);
                }

                decimal totalRow = 0;
                foreach (DataColumn col in baseTable.Columns)
                {
                    var colName = col.ColumnName;
                    if (colName == "Chukì" || colName == "Col2" || colName == "Tổng") continue;

                    var valStr = newRow[colName]?.ToString();
                    if (decimal.TryParse(valStr?.Replace(",", ""), out var val))
                    {
                        totalRow += val;
                    }
                }
                newRow["Tổng"] = totalRow.ToString("#,##0", CultureInfo.InvariantCulture);

                resultTable.Rows.Add(newRow);
            }

            var columnsSql = resultTable.Columns
                .Cast<DataColumn>()
                .Select(c => $"[{c.ColumnName}] NVARCHAR(MAX)");
            var createSql = $"CREATE TABLE [{safeTableName}] ({string.Join(", ", columnsSql)})";
            using var createCmd = new SqlCommand(createSql, conn);
            await createCmd.ExecuteNonQueryAsync();

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = safeTableName
            };
            foreach (DataColumn col in resultTable.Columns)
                bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
            await bulkCopy.WriteToServerAsync(resultTable);
        }

        public async Task ExtractSundayRowsAsync(string sourceTable, string targetTable)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            var safeSource = Regex.Replace(sourceTable, @"[^\w]", "");
            var safeTarget = Regex.Replace(targetTable, @"[^\w]", "");

            var checkTableCmd = new SqlCommand(
                $"IF OBJECT_ID('{safeTarget}', 'U') IS NULL SELECT 0 ELSE SELECT 1", conn);

            var exists = (int)await checkTableCmd.ExecuteScalarAsync();

            if (exists == 0)
            {
                var createCmd = new SqlCommand($@"
                SELECT * INTO [{safeTarget}]
                FROM [{safeSource}]
                WHERE Col2 = 'Sunday'
                ", conn);
                await createCmd.ExecuteNonQueryAsync();
            }
            else
            {
                var insertCmd = new SqlCommand($@"
                INSERT INTO [{safeTarget}]
                SELECT * FROM [{safeSource}]
                WHERE Col2 = 'Sunday'
                AND NOT EXISTS (
                SELECT 1 FROM [{safeTarget}]
                WHERE [{safeTarget}].Chukì = [{safeSource}].Chukì
                )
                ", conn);
                await insertCmd.ExecuteNonQueryAsync();
            }
        }

        private string NormalizeDate(object? input)
        {
            if (input == null) return "";

            string raw = input.ToString()?.Trim() ?? "";

            string[] formats = { "dd/MM/yyyy", "d/M/yyyy", "M/d/yyyy", "yyyy-MM-dd" };

            if (DateTime.TryParseExact(raw, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                return dt.ToString("d/M/yyyy", CultureInfo.InvariantCulture);

            if (DateTime.TryParse(raw, out dt))
                return dt.ToString("d/M/yyyy", CultureInfo.InvariantCulture);

            return raw;
        }
    }
}