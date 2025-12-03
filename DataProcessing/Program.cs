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

using QuantConnect.Configuration;
using QuantConnect.Logging;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.IO;
using QuantConnect.DataSource;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Entry point for Brain dataset processors following Lean DataFleet standards.
    /// </summary>
    public class Program
    {
        public static void Main(string[] args)
        {
            string dataset = null;

            try
            {
                for (int i = 0; i < args.Length; i++)
                {
                    switch (args[i].ToLowerInvariant())
                    {
                        case "--dataset":
                            dataset = args[++i];
                            break;

                        case "--help":
                        case "-h":
                            PrintHelp();
                            Environment.Exit(0);
                            break;
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed parsing arguments");
                PrintHelp();
                Environment.Exit(1);
            }

            if (string.IsNullOrWhiteSpace(dataset))
            {
                Log.Error("Missing --dataset argument");
                PrintHelp();
                Environment.Exit(1);
            }

            // ------------------------------------------------------------
            // Read required environment variables
            // ------------------------------------------------------------
            var deploymentDateValue = Environment.GetEnvironmentVariable("QC_DATAFLEET_DEPLOYMENT_DATE");
            if (string.IsNullOrWhiteSpace(deploymentDateValue))
            {
                Log.Error("QC_DATAFLEET_DEPLOYMENT_DATE environment variable missing.");
                Environment.Exit(1);
            }

            var deploymentDate = Parse.DateTimeExact(deploymentDateValue, "yyyyMMdd");

            var awsAccessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
            var awsSecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");
            var bucket = Environment.GetEnvironmentVariable("BRAIN_S3_BUCKET");

            if (string.IsNullOrWhiteSpace(awsAccessKeyId) ||
                string.IsNullOrWhiteSpace(awsSecretAccessKey) ||
                string.IsNullOrWhiteSpace(bucket))
            {
                Log.Error("Missing AWS credentials or BRAIN_S3_BUCKET.");
                Environment.Exit(1);
            }

            // ------------------------------------------------------------
            // Output directory
            // ------------------------------------------------------------
            //var outputRoot = Path.Combine(Config.Get("temp-output-directory", "/temp-output-directory"),"alternative");
            var outputRoot = "/Users/ashutosh/Documents/GitHub/Lean.DataSource.BrainSentiment/DataProcessing/Output";

            Directory.CreateDirectory(outputRoot);

            Log.Trace($"Starting Brain dataset processor for dataset={dataset}, date={deploymentDate:yyyyMMdd}");

            var converters = new List<IBrainDataConverter>();

            try
            {
                switch (dataset.ToLowerInvariant())
                {
                    case "blmect":
                        converters.Add(
                            new BrainLanguageMetricsEarningsCallsConverter(
                                awsAccessKeyId,
                                awsSecretAccessKey,
                                bucket,
                                outputRoot
                            )
                        );
                        break;

                    case "bwpv":
                        converters.Add(
                            new BrainWikipediaPageViewsConverter(
                                awsAccessKeyId,
                                awsSecretAccessKey,
                                bucket,
                                outputRoot
                            )
                        );
                        break;

                    default:
                        Log.Error($"Unknown dataset '{dataset}'");
                        PrintHelp();
                        Environment.Exit(1);
                        break;
                }
            }
            catch (Exception e)
            {
                Log.Error(e, "Failed constructing converter");
                Environment.Exit(1);
            }

            var success = true;

            foreach (var converter in converters)
            {
                try
                {
                    if (!converter.ProcessDate(deploymentDate))
                    {
                        Log.Error($"Failed to process dataset={dataset}");
                        success = false;
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e, $"Converter for {dataset} crashed unexpectedly");
                    success = false;
                }

                converter.DisposeSafely();
            }

            Environment.Exit(success ? 0 : 1);
        }

        private static void PrintHelp()
        {
            Log.Trace("Usage:");
            Log.Trace("  dotnet process.dll --dataset <BLMECT|BWPV>");
        }
    }

    /// <summary>
    /// Shared interface for Brain converters.
    /// </summary>
    public interface IBrainDataConverter : IDisposable
    {
        bool ProcessDate(DateTime date);
    }
}
