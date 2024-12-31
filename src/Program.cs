using System.Globalization;
using System.CommandLine;
using CsvHelper.Configuration;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using System.Reflection;
using Microsoft.Data.SqlClient;
using System.Data;

public class Program
{
  //Connection with SQL Server (put your connecntion string)
  private static string _connectionString =
  "Server=<SERVER>;Database=test_assessment_db;Trusted_Connection=True;User Id=ETL;Password=2024; TrustServerCertificate=True";
  private static string sqlTableName = "DataFromCSV";

  static async Task<int> Main(string[] args)
  {

    var readPathOption = new Option<string?>(
            name: "--path",
            description: "The CVS file path.");

    var writePathOption = new Option<string?>(
            name: "--duplicate-path",
            description: "The CVS duplicate path.");

    readPathOption.IsRequired = true;
    readPathOption.AddAlias("--p");

    writePathOption.IsRequired = true;
    writePathOption.AddAlias("--d");

    var rootCommand = new RootCommand("Simple ETL project in CLI");
    rootCommand.AddOption(readPathOption);
    rootCommand.AddOption(writePathOption);


    rootCommand.SetHandler((PathRead, PathWrite) =>
        {
          HandleDataFromCSV(PathRead!, PathWrite!);
        },
        readPathOption, writePathOption);

    return await rootCommand.InvokeAsync(args);
  }

  static private void HandleDataFromCSV(string pathRead, string pathWrite)
  {
    var csvConfig = new CsvConfiguration(cultureInfo: CultureInfo.InvariantCulture)
    {
      MissingFieldFound = null,
      ShouldSkipRecord = args => args.Row.Parser.Record!.Any(field => string.IsNullOrEmpty(field))
    };

    using (var reader = new StreamReader(pathRead))
    using (var csv = new CsvReader(reader, csvConfig))
    {
      var records = csv.GetRecords<Record>().ToList();

      var duplicate = records
               .GroupBy(r => new { r.TpepPickupDatetime, r.TpepDropoffDatetime, r.PassengerCount })
               .Where(g => g.Count() > 1)
               .SelectMany(g => g)
               .ToList();

      if (duplicate.Count != 0)
        WriteDuplicate2CSV(duplicate, pathWrite);

      var recordList = records
              .GroupBy(r => new { r.TpepPickupDatetime, r.TpepDropoffDatetime, r.PassengerCount })
              .Select(g => g.First())
              .ToList();

      IsnertData2DB(recordList);
    }

  }

  static private void IsnertData2DB(List<Record> records)
  {
    using (var connection = new SqlConnection(_connectionString))
    {
      connection.Open();

      var dataTable = new DataTable();

      var temp = typeof(Record)
          .GetProperties()
          .Select(prop => new
          {
            NameP = prop.Name,
            NameA = prop.GetCustomAttribute<NameAttribute>()!.Names.First(),
            TypeP = prop.PropertyType
          })
          .ToList();

      foreach (var t in temp)
      {
        dataTable.Columns.Add(t.NameA, t.TypeP);
      }

      foreach (var data in records)
      {
        var row = dataTable.NewRow();

        foreach (var t in temp)
        {
          var propertyValue = typeof(Record).GetProperty(t.NameP)?.GetValue(data);

          row[t.NameA] = propertyValue ?? DBNull.Value;
        }

        dataTable.Rows.Add(row);
      }

      using (var bulkCopy = new SqlBulkCopy(connection))
      {
        bulkCopy.DestinationTableName = sqlTableName;

        bulkCopy.WriteToServer(dataTable);
      }
    }
  }

  static private void WriteDuplicate2CSV(List<Record> duplicate, string path)
  {
    using (var writer = new StreamWriter(path + "/duplicates.csv"))
    using (var csvWrite = new CsvWriter(writer, CultureInfo.InvariantCulture))
    {
      csvWrite.WriteRecords(duplicate);
    }
  }

}




