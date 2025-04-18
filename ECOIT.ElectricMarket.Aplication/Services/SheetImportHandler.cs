﻿using ECOIT.ElectricMarket.Aplication.Interface;
using ECOIT.ElectricMarket.Application.Interface;
using OfficeOpenXml;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace ECOIT.ElectricMarket.Application.Services;

public class SheetImportHandler : ISheetImportHandler
{
    private readonly IDynamicTableService _tableService;
    public SheetImportHandler(IDynamicTableService tableService)
    {
        _tableService = tableService;
    }

    private static string NormalizeVietnamese(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;

        var normalized = input.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder();

        foreach (var ch in normalized)
        {
            var unicodeCategory = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (unicodeCategory != UnicodeCategory.NonSpacingMark)
                sb.Append(ch);
        }

        return sb.ToString().Normalize(NormalizationForm.FormC);
    }

    public async Task ImportSheetAsync(Stream fileStream, string sheetName, int headerRow, int startRow, string? tableName = null, int? endRow = null)
    {
        ExcelPackage.LicenseContext = OfficeOpenXml.LicenseContext.NonCommercial;
        Console.WriteLine(tableName);
        using var package = new ExcelPackage(fileStream);
        var sheet = package.Workbook.Worksheets[sheetName];

        if (sheet == null)
            throw new Exception($"Không tìm thấy sheet tên '{sheetName}'");

        int endCol = sheet.Dimension.End.Column;
        int maxRow = sheet.Dimension.End.Row;
        int actualEndRow = endRow ?? maxRow;
        Console.WriteLine(actualEndRow);
        var columns = new List<string>();
        for (int col = 1; col <= endCol; col++)
        {
            var header = sheet.Cells[headerRow, col].Text.Trim();
            columns.Add(string.IsNullOrWhiteSpace(header) ? $"Col{col}" : header);
        }

        var rows = new List<List<string>>();
        for (int row = startRow; row <= actualEndRow; row++)
        {
            var rowData = new List<string>();
            for (int col = 1; col <= endCol; col++)
            {
                rowData.Add(sheet.Cells[row, col].Text);
            }
            rows.Add(rowData);
        }
        var finalTableName = string.IsNullOrWhiteSpace(tableName) ? sheetName : tableName;
        var normalizedTableName = Regex.Replace(NormalizeVietnamese(finalTableName), @"\W+", "");

        await _tableService.CreateTableAsync(normalizedTableName, columns);
        await _tableService.InsertDataAsync(normalizedTableName, columns, rows);
    }
}
