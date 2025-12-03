using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using QuantConnect;
using QuantConnect.Logging;
using QuantConnect.DataProcessing;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Converts Brain Wikipedia Page Views (BWPV) raw S3 files into Lean-format:
    ///     bwpv/{yyyyMM}/{symbol}.csv
    ///
    /// Raw file pattern:
    ///     s3://{bucket}/BWPV/metrics_YYYYMMDD.csv
    /// </summary>
    public class BrainWikipediaPageViewsConverter : IBrainDataConverter
    {
        private readonly IAmazonS3 _s3Client;
        private readonly string _bucket;
        private readonly string _outputRoot;

        public BrainWikipediaPageViewsConverter(
            string awsAccessKeyId,
            string awsSecretAccessKey,
            string bucket,
            string outputRoot)
        {
            _bucket = bucket;
            _outputRoot = outputRoot;

            _s3Client = new AmazonS3Client(
                awsAccessKeyId,
                awsSecretAccessKey,
                RegionEndpoint.USEast1
            );
        }

        /// <summary>
        /// Processes a single deployment date file.
        /// </summary>
        public bool ProcessDate(DateTime date)
        {
            var dateString = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var key = $"BWPV/metrics_{dateString}.csv";

            Log.Trace($"[BWPV] Downloading s3://{_bucket}/{key}");

            GetObjectResponse response;

            try
            {
                response = _s3Client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = key
                }).Result;
            }
            catch (Exception err)
            {
                Log.Error(err, $"[BWPV] Failed to download key {key}");
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
                    Log.Error("[BWPV] Empty header line.");
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
                        list = new List<string>();
                        rowsBySymbol[ticker] = list;
                    }

                    list.Add(outRow);
                }
            }
            catch (Exception err)
            {
                Log.Error(err, "[BWPV] Failed while parsing CSV.");
                return false;
            }
            
            var outDir = Path.Combine(_outputRoot, "bwpv");

            Directory.CreateDirectory(outDir);

            foreach (var kvp in rowsBySymbol)
            {
                var ticker = kvp.Key.ToLowerInvariant();
                var filePath = Path.Combine(outDir, $"{ticker}.csv");

                try
                {
                    using var writer = new StreamWriter(filePath, append: true);
                    foreach (var row in kvp.Value)
                        writer.WriteLine(row);
                }
                catch (Exception err)
                {
                    Log.Error(err, $"[BWPV] Failed writing file {filePath}");
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Required by the IBrainDataConverter interface.
        /// </summary>
        public void Dispose()
        {
            // Nothing to dispose right now.
        }
    }
}
