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

using Amazon.S3.Model;
using QuantConnect.Logging;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Converts Brain Language Metrics on Earnings Calls (BLMECT)
    /// raw S3 files into Lean-format:
    ///
    ///     blmect/{yyyyMMdd}/{symbol}.csv
    ///
    /// Raw Keys:
    ///     BLMECT/metrics_earnings_call_YYYYMMDD.csv
    ///     BLMECT/differences_earnings_call_YYYYMMDD.csv
    /// </summary>
    public class BrainLanguageMetricsEarningsCallsConverter(string outputRoot) 
        : BrainDataConverter("blmect", outputRoot)
    {
        /// <summary>
        /// Converts all available deployment dates.
        /// </summary>
        public override bool ProcessHistory()
        {
            var dates = new List<DateTime>();
            var req = new ListObjectsV2Request
            {
                BucketName = BucketName,
                Prefix = $"{Prefix}/"
            };

            ListObjectsV2Response resp;
            
            do
            {
                resp = S3Client.ListObjectsV2Async(req).GetAwaiter().GetResult();
                var s3Objects = resp.S3Objects;
                dates.AddRange(s3Objects.Where(x => x.Key.StartsWith($"{Prefix}/differences_earnings_call_")).Select(x => DateTime.ParseExact(x.Key[33..41], "yyyyMMdd", CultureInfo.InvariantCulture)));
                dates.AddRange(s3Objects.Where(x => x.Key.StartsWith($"{Prefix}/metrics_earnings_call_")).Select(x => DateTime.ParseExact(x.Key[29..37], "yyyyMMdd", CultureInfo.InvariantCulture)));
                req.ContinuationToken = resp.NextContinuationToken;
            }
            while (resp.IsTruncated);
            
            Log.Trace($"[{Prefix}] Found {dates.Distinct().Count()} unique deployment dates.");
            return dates.Distinct().OrderBy(x => x).All(ProcessDate);
        }

        /// <summary>
        /// Converts all files for the given deployment date.
        /// </summary>
        public override bool ProcessDate(DateTime date)
        {
            var fileDate = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

            // ------------------------------------------------------------
            // 1. Load the DIFF file
            // ------------------------------------------------------------
            var diffKey = $"{Prefix}/differences_earnings_call_{fileDate}.csv";
            var diffByTicker = new Dictionary<string, string[]>();

            Log.Trace($"[{Prefix}] Downloading DIFF: s3://***/{diffKey}");

            try
            {
                using var diffResponse = S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = diffKey
                }).Result;

                using var reader = new StreamReader(diffResponse.ResponseStream);

                var header = reader.ReadLine();
                var delimiter = header.Contains('\t') ? '\t' : ',';

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(delimiter);
                    if (parts.Length < 47)
                    {
                        Log.Trace($"[{Prefix}] DIFF skipped row (too few columns): {parts.Length}");
                        continue;
                    }

                    var ticker = parts[1];
                    if (!string.IsNullOrWhiteSpace(ticker))
                        diffByTicker[ticker] = parts;
                }
            }
            catch (Exception err)
            {
                Log.Trace($"[{Prefix}] DIFF optional file missing for {fileDate}: {err.Message}");
            }

            // ------------------------------------------------------------
            // 2. Load the METRICS file
            // ------------------------------------------------------------
            var metricsKey = $"{Prefix}/metrics_earnings_call_{fileDate}.csv";

            Log.Trace($"[{Prefix}] Downloading METRICS: s3://***/{metricsKey}");

            Dictionary<string, List<string>> rowsBySymbol = [];

            GetObjectResponse metricsResponse;
            try
            {
                metricsResponse = S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = metricsKey
                }).Result;
            }
            catch (Exception err)
            {
                Log.Error(err, $"[{Prefix}] Failed to download METRICS file for {fileDate}");
                return false;
            }

            try
            {
                using var reader = new StreamReader(metricsResponse.ResponseStream);

                var header = reader.ReadLine();
                var delimiter = header.Contains('\t') ? '\t' : ',';

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(delimiter);
                    if (parts.Length < 29)
                    {
                        Log.Trace($"[{Prefix}] METRICS skipped row (too few columns): {parts.Length}");
                        continue;
                    }

                    var ticker = parts[1];
                    if (string.IsNullOrWhiteSpace(ticker))
                        continue;

                    diffByTicker.TryGetValue(ticker, out var diffRow);

                    var outRow = BuildOutputRow(fileDate, parts, diffRow);

                    if (!rowsBySymbol.TryGetValue(ticker, out var list))
                    {
                        rowsBySymbol[ticker] = list = [];
                    }

                    list.Add(outRow);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, $"[{Prefix}] Failed parsing METRICS CSV.");
                return false;
            }

            try
            {
                rowsBySymbol.DoForEach(kvp => SaveContentToFile(kvp.Key, kvp.Value));
            }
            catch (Exception err)
            {
                Log.Error(err, $"[{Prefix}] Failed writing output files.");
                return false;
            }

            Log.Trace($"[{Prefix}] Completed fileDate={fileDate}: {rowsBySymbol.Count} symbols written.");

            return true;
        }

        /// <summary>
        /// Build the combined metrics+diff output record.
        /// </summary>
        private static string BuildOutputRow(string fileDate, string[] metrics, string[] diff)
        {
            var f = new List<string>
            {
                // 0 - snapshot date
                fileDate,

                // metrics fields
                DateTime.TryParse(metrics[3], out var dt) ? dt.ToString("yyyyMMdd") : "",
                metrics[4], // quarter
                metrics[5] // year
            };

            for (int i = 6; i < 15; i++) f.Add(metrics[i]);
            for (int i = 15; i < 20; i++) f.Add(metrics[i]);
            for (int i = 20; i < 29; i++) f.Add(metrics[i]);

            // DIFF fields (if available)
            if (diff != null)
            {
                f.Add(DateTime.TryParse(diff[6], out var dtPrev) ? dtPrev.ToString("yyyyMMdd") : "");
                f.Add(diff[7]); // prev quarter
                f.Add(diff[8]); // prev year

                for (int i = 9; i <= 23; i++) f.Add(diff[i]);
                for (int i = 24; i <= 31; i++) f.Add(diff[i]);
                for (int i = 32; i <= 46; i++) f.Add(diff[i]);
            }
            else
            {
                // Blank padding: 41 fields
                for (int i = 0; i < 41; i++)
                    f.Add("");
            }

            return string.Join(",", f);
        }
    }
}
