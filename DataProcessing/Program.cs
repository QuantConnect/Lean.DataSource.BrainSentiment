/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Globalization;
using QuantConnect.DataSource;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// CLI entrypoint for Brain dataset processing.
    /// Supports:
    ///   --dataset BLMECT
    ///   --dataset BWPV
    ///   --date YYYYMMDD
    /// </summary>
    public static class Program
    {
        public static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                PrintHelp();
                return;
            }

            string dataset = null;
            DateTime date = DateTime.MinValue;

            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i].ToLowerInvariant())
                {
                    case "--dataset":
                        dataset = args[++i];
                        break;

                    case "--date":
                        string ds = args[++i];
                        date = DateTime.ParseExact(ds, "yyyyMMdd", CultureInfo.InvariantCulture);
                        break;

                    case "--help":
                    case "-h":
                        PrintHelp();
                        return;
                }
            }

            if (string.IsNullOrWhiteSpace(dataset))
            {
                Console.WriteLine("[ERROR] Missing --dataset argument");
                PrintHelp();
                return;
            }

            if (date == DateTime.MinValue)
            {
                Console.WriteLine("[ERROR] Missing or invalid --date YYYYMMDD argument");
                PrintHelp();
                return;
            }

            Console.WriteLine($"[INFO] Running dataset={dataset}, date={date:yyyyMMdd}");

            string bucket = Environment.GetEnvironmentVariable("BRAIN_S3_BUCKET");

            string outputRoot = AppDomain.CurrentDomain.BaseDirectory;

            switch (dataset.ToLowerInvariant())
            {
                case "blmect":
                    Console.WriteLine("[INFO] Starting BLMECT converter...");
                    var c1 = new BrainLanguageMetricsEarningsCallsConverter(bucket, outputRoot);
                    c1.ProcessDate(date);
                    break;

                case "bwpv":
                    Console.WriteLine("[INFO] Starting BWPV converter...");
                    var c2 = new BrainWikipediaPageViewsConverter(bucket, outputRoot);
                    c2.ProcessDate(date);
                    break;

                default:
                    Console.WriteLine($"[ERROR] Unknown dataset: {dataset}");
                    PrintHelp();
                    return;
            }

            Console.WriteLine("[INFO] Completed.");
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  dotnet process.dll --dataset <BLMECT|BWPV> --date YYYYMMDD");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  dotnet process.dll --dataset BLMECT --date 20250901");
            Console.WriteLine("  dotnet process.dll --dataset BWPV  --date 20241211");
        }
    }
}
