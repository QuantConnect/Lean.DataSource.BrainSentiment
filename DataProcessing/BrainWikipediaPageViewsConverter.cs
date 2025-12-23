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
    /// Converts Brain Wikipedia Page Views (BWPV) raw S3 files into Lean-format:
    ///     bwpv/{yyyyMM}/{symbol}.csv
    ///
    /// Raw file pattern:
    ///     s3://{bucket}/BWPV/metrics_YYYYMMDD.csv
    /// </summary>
    public class BrainWikipediaPageViewsConverter(string outputRoot)
        : BrainDataConverter("bwpv", outputRoot)
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
                dates.AddRange(s3Objects.Where(x => x.Key.StartsWith($"{Prefix}/metrics_")).Select(x => DateTime.ParseExact(x.Key[13..21], "yyyyMMdd", CultureInfo.InvariantCulture)));
                req.ContinuationToken = resp.NextContinuationToken;
            }
            while (resp.IsTruncated);
            
            Log.Trace($"[{Prefix}] Found {dates.Distinct().Count()} unique deployment dates.");
            return dates.Distinct().OrderBy(x => x).All(ProcessDate);
        }

        /// <summary>
        /// Processes a single deployment date file.
        /// </summary>
        public override bool ProcessDate(DateTime date)
        {
            var fileDate = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var key = $"{Prefix}/metrics_{fileDate}.csv";

            Log.Trace($"[{Prefix}] Downloading s3://***/{key}");

            GetObjectResponse response;

            try
            {
                response = S3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = BucketName,
                    Key = key
                }).Result;
            }
            catch (Exception err)
            {
                Log.Error(err, $"[{Prefix}] Failed to download key {key}");
                return false;
            }

            var rowsBySymbol = new Dictionary<string, List<string>>();

            try
            {
                using var stream = response.ResponseStream;
                using var reader = new StreamReader(stream);

                var header = reader.ReadLine();
                if (string.IsNullOrWhiteSpace(header))
                {
                    Log.Error($"[{Prefix}] Empty header line.");
                    return false;
                }

                var delimiter = header.Contains('\t') ? '\t' : ',';

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var parts = line.Split(delimiter);
                    if (parts.Length < 9)
                        continue;

                    var ticker = parts[1];
                    if (string.IsNullOrWhiteSpace(ticker))
                        continue;

                    var rawDate = parts[2];
                    if (!DateTime.TryParse(rawDate, out var dt))
                        continue;

                    var dateStr = dt.ToString("yyyyMMdd");

                    var outRow = string.Join(",", new[]
                    {
                        dateStr,
                        parts[3], // NUMBER_VIEWS_1
                        parts[4], // BUZZ_1
                        parts[5], // NUMBER_VIEWS_7
                        parts[6], // BUZZ_7
                        parts[7], // NUMBER_VIEWS_30
                        parts[8]  // BUZZ_30
                    });

                    if (!rowsBySymbol.TryGetValue(ticker, out var list))
                    {
                        rowsBySymbol[ticker] = list = [];
                    }

                    list.Add(outRow);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, $"[{Prefix}] Failed while parsing CSV.");
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
    }
}
