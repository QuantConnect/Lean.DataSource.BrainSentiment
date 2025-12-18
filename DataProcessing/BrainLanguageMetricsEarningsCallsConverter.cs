using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using QuantConnect.Logging;
using QuantConnect.DataProcessing;
using System.Linq;

namespace QuantConnect.DataSource
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
    public class BrainLanguageMetricsEarningsCallsConverter : IBrainDataConverter
    {
        private readonly IAmazonS3 _s3;
        private readonly string _bucket;
        private readonly string _outputRoot;

        public BrainLanguageMetricsEarningsCallsConverter(
            string awsAccessKeyId,
            string awsSecretAccessKey,
            string bucket,
            string outputRoot)
        {
            _bucket = bucket;
            _outputRoot = outputRoot;

            _s3 = new AmazonS3Client(
                awsAccessKeyId,
                awsSecretAccessKey,
                RegionEndpoint.USEast1
            );
        }

        /// <summary>
        /// Converts all available deployment dates.
        /// </summary>
        public bool ProcessHistory()
        {
            var dates = new List<DateTime>();
            var req = new ListObjectsV2Request
            {
                BucketName = _bucket,
                Prefix = "BLMECT/"
            };

            ListObjectsV2Response resp;
            
            do
                {
                    resp = _s3.ListObjectsV2Async(req).GetAwaiter().GetResult();
                    var s3Objects = resp.S3Objects;
                    dates.AddRange(s3Objects.Where(x => x.Key.StartsWith("BLMECT/differences_earnings_call_")).Select(x => DateTime.ParseExact(x.Key[33..41], "yyyyMMdd", CultureInfo.InvariantCulture)));
                    dates.AddRange(s3Objects.Where(x => x.Key.StartsWith("BLMECT/metrics_earnings_call_")).Select(x => DateTime.ParseExact(x.Key[29..37], "yyyyMMdd", CultureInfo.InvariantCulture)));
                    req.ContinuationToken = resp.NextContinuationToken;
                }
            while (resp.IsTruncated);
            
            Log.Trace($"[BLMECT] Found {dates.Distinct().Count()} unique deployment dates.");
            return dates.Distinct().OrderBy(x => x).All(ProcessDate);
        }

        /// <summary>
        /// Converts all files for the given deployment date.
        /// </summary>
        public bool ProcessDate(DateTime date)
        {
            var fileDate = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

            // ------------------------------------------------------------
            // 1. Load the DIFF file
            // ------------------------------------------------------------
            var diffKey = $"BLMECT/differences_earnings_call_{fileDate}.csv";
            var diffByTicker = new Dictionary<string, string[]>();

            Log.Trace($"[BLMECT] Downloading DIFF: s3://{_bucket}/{diffKey}");

            try
            {
                using var diffResponse = _s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = _bucket,
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
                        Log.Trace($"[BLMECT] DIFF skipped row (too few columns): {parts.Length}");
                        continue;
                    }

                    var ticker = parts[1];
                    if (!string.IsNullOrWhiteSpace(ticker))
                        diffByTicker[ticker] = parts;
                }
            }
            catch (Exception err)
            {
                Log.Trace($"[BLMECT] DIFF optional file missing for {fileDate}: {err.Message}");
            }

            // ------------------------------------------------------------
            // 2. Load the METRICS file
            // ------------------------------------------------------------
            var metricsKey = $"BLMECT/metrics_earnings_call_{fileDate}.csv";

            Log.Trace($"[BLMECT] Downloading METRICS: s3://{_bucket}/{metricsKey}");

            Dictionary<string, List<string>> rowsBySymbol = new();

            GetObjectResponse metricsResponse;
            try
            {
                metricsResponse = _s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = metricsKey
                }).Result;
            }
            catch (Exception err)
            {
                Log.Error(err, $"[BLMECT] Failed to download METRICS file for {fileDate}");
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
                        Log.Trace($"[BLMECT] METRICS skipped row (too few columns): {parts.Length}");
                        continue;
                    }

                    var ticker = parts[1];
                    if (string.IsNullOrWhiteSpace(ticker))
                        continue;

                    diffByTicker.TryGetValue(ticker, out var diffRow);

                    var outRow = BuildOutputRow(fileDate, parts, diffRow);

                    if (!rowsBySymbol.TryGetValue(ticker, out var list))
                    {
                        list = new List<string>();
                        rowsBySymbol[ticker] = list;
                    }

                    list.Add(outRow);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, "[BLMECT] Failed parsing METRICS CSV.");
                return false;
            }

            var outDir = Path.Combine(_outputRoot, "blmect");
            Directory.CreateDirectory(outDir);

            try
            {
                foreach (var kvp in rowsBySymbol)
                {
                    var ticker = kvp.Key.ToLowerInvariant();
                    var filePath = Path.Combine(outDir, $"{ticker}.csv");

                    using var writer = new StreamWriter(filePath, append: true);
                    foreach (var row in kvp.Value)
                        writer.WriteLine(row);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, "[BLMECT] Failed writing output files.");
                return false;
            }

            Log.Trace($"[BLMECT] Completed fileDate={fileDate}: {rowsBySymbol.Count} symbols written.");

            return true;
        }

        /// <summary>
        /// Build the combined metrics+diff output record.
        /// </summary>
        private static string BuildOutputRow(string fileDate, string[] metrics, string[] diff)
        {
            var f = new List<string>();

            // 0 - snapshot date
            f.Add(fileDate);

            // metrics fields
            f.Add(DateTime.TryParse(metrics[3], out var dt) ? dt.ToString("yyyyMMdd") : "");
            f.Add(metrics[4]); // quarter
            f.Add(metrics[5]); // year

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

        public void Dispose()
        {
            // nothing to clean up
        }
    }
}
