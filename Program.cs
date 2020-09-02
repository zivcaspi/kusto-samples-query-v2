using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Utils;

using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Data.Results;

namespace kusto_samples_query_v2
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                MainImpl();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception raised: {0}", ex.ToString());
            }
        }

        static void MainImpl()
        {
            // 1. Create a connection string to a cluster/database with AAD user authentication
            var cluster = "https://help.kusto.windows.net/";
            var database = "Samples";
            var kcsb = new KustoConnectionStringBuilder(cluster, database)
            {
                FederatedSecurity = true
            };

            // 2. Connect to the Kusto query endpoint and create a query provider
            using (var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb))
            {
                // 3. Send a query using the V2 API
                var query = "print Welcome='Hello, World!'; print PI=pi()";
                var properties = new ClientRequestProperties()
                {
                    ClientRequestId = "kusto_samples_query_v2;" + Guid.NewGuid().ToString()
                };
                var queryTask = queryProvider.ExecuteQueryV2Async(database, query, properties);

                // 4. Parse and print the results of the query
                WriteResultsToConsole(queryTask);
            }
        }

        static void WriteResultsToConsole(Task<ProgressiveDataSet> queryTask)
        {
            using (var dataSet = queryTask.Result)
            {
                using (var frames = dataSet.GetFrames())
                {
                    var frameNum = 0;
                    while (frames.MoveNext())
                    {
                        var frame = frames.Current;
                        WriteFrameResultsToConsole(frameNum++, frame);
                    }
                }
            }
        }

        static void WriteFrameResultsToConsole(int frameNum, ProgressiveDataSetFrame frame)
        {
            var tableKind = WellKnownDataSet.Unknown;

            switch (frame.FrameType)
            {
                case FrameType.DataSetHeader:
                    {
                        // This is the first frame we'll get back
                        var frameex = frame as ProgressiveDataSetHeaderFrame;
                        Console.WriteLine($"DataSetHeader: Version={frameex.Version}");
                        Console.WriteLine();
                    }
                    break;

                case FrameType.TableHeader:
                    // If progressive results are enabled, this is a one-time header
                    // appearing before each data table.
                    // In this example progressive results are not used.
                    break;

                case FrameType.TableFragment:
                    // If progressive results are enabled, this is a frame that provides
                    // parts of the table's data.
                    // In this example progressive results are not used.
                    break;

                case FrameType.TableCompletion:
                    // If progressive results are enabled, this is a frame that provides
                    // parts of the table's data.
                    // In this example progressive results are not used.
                    break;

                case FrameType.TableProgress:
                    // If progressive results are enabled, this is a frame that provides
                    // an indication for how much progress was made in returning results.
                    // In this example progressive results are not used.
                    break;

                case FrameType.DataTable:
                    {
                        // This frame represents one data table (in all, when progressive results
                        // are not used or there's no need for multiple-frames-per-table).
                        // There are usually multiple such tables in the response, differentiated
                        // by purpose (TableKind).
                        // Note that we can't skip processing the data -- we must consume it.

                        var frameex = frame as ProgressiveDataSetDataTableFrame;
                        tableKind = frameex.TableKind;
                        var banner = $"[{frameNum}] DataTable(DataTableFrame): TableId={frameex.TableId}, TableName={frameex.TableName}, TableKind={frameex.TableKind}";
                        WriteResults(banner, frameex.TableData);
                    }
                    break;

                case FrameType.DataSetCompletion:
                    {
                        // This is the last frame in the data set.
                        // It provides information on the overall success of the query:
                        // Whether there were any errors, whether it got cancelled mid-stream,
                        // and what exceptions were raised if either is true.
                        var frameex = frame as ProgressiveDataSetCompletionFrame;
                        Console.WriteLine($"[{frameNum}] DataSetCompletion(CompletionFrame): HasErrors={frameex.HasErrors}, Cancelled={frameex.Cancelled}, Exception={ExtendedString.SafeToString(frameex.Exception)}");
                    }
                    break;

                case FrameType.LastInvalid:
                default:
                    // In general this should not happen
                    break;
            }
        }

        static void WriteResults(string banner, IDataReader reader)
        {
            var writer = new System.IO.StringWriter();
            reader.WriteAsText(banner, true, writer,
                firstOnly: false,
                markdown: false,
                includeWithHeader: "ColumnType",
                includeHeader: true);
            Console.WriteLine(writer.ToString());
        } // WriteResults
    }
}
