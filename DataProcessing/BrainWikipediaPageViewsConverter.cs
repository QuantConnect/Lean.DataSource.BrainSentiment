using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using QuantConnect;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Helper class to convert raw BWPV files from S3 into Lean-ready
    /// monthly CSV files in:
    ///     alternative/brain/bwpv/{yyyyMM}/{symbol}.csv
    ///
    /// Raw S3:
    ///   bucket: ashutosh-dev-brain
    ///   prefix: BWPV/metrics_YYYYMMDD.csv
    /// </summary>
    public class BrainWikipediaPageViewsConverter
    {
        private readonly IAmazonS3 _s3Client;
        private readonly string _bucket;
        private readonly string _outputRoot;

        public BrainWikipediaPageViewsConverter(string bucket, string outputRoot)
        {
            _bucket = bucket;
            _outputRoot = outputRoot;
            _s3Client = new AmazonS3Client(RegionEndpoint.USEast1);
        }

        public void ProcessDate(DateTime date)
        {
            var dateString = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var key = $"BWPV/metrics_{dateString}.csv";

            Console.WriteLine($"[BWPV] Downloading s3://{_bucket}/{key}");

            using var response = _s3Client.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucket,
                Key = key
            }).Result;

            using var stream = response.ResponseStream;
            using var reader = new StreamReader(stream);

            var headerLine = reader.ReadLine();
            if (string.IsNullOrWhiteSpace(headerLine))
            {
                Console.WriteLine("[BWPV] Empty header line");
                return;
            }

            var delimiter = headerLine.Contains('\t') ? '\t' : ',';
            var rowsBySymbol = new Dictionary<string, List<string>>();

            string line;
            while ((line = reader.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var parts = line.Split(delimiter);
                if (parts.Length < 8)
                {
                    continue;
                }

                // Raw layout:
                // 0: COMPOSITE_FIGI
                // 1: TICKER
                // 2: DATE
                // 3: NUMBER_VIEWS_1
                // 4: BUZZ_1
                // 5: NUMBER_VIEWS_7
                // 6: BUZZ_7
                // 7: NUMBER_VIEWS_30
                // 8: BUZZ_30
                var ticker = parts[1];
                if (string.IsNullOrWhiteSpace(ticker))
                {
                    continue;
                }

                var dateField = parts[2];
                var dt = DateTime.Parse(dateField, CultureInfo.InvariantCulture);
                var dateStr = dt.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

                var outputFields = new[]
                {
                    dateStr,
                    parts[3], // NUMBER_VIEWS_1
                    parts[4], // BUZZ_1
                    parts[5], // NUMBER_VIEWS_7
                    parts[6], // BUZZ_7
                    parts[7], // NUMBER_VIEWS_30
                    parts[8]  // BUZZ_30
                };

                if (!rowsBySymbol.TryGetValue(ticker, out var list))
                {
                    list = new List<string>();
                    rowsBySymbol[ticker] = list;
                }

                list.Add(string.Join(",", outputFields));
            }

            foreach (var kvp in rowsBySymbol)
            {
                var ticker = kvp.Key;
                var lines = kvp.Value;

                var outDir = Path.Combine(_outputRoot, "alternative", "brain", "bwpv");
                Directory.CreateDirectory(outDir);
                
                var filePath = Path.Combine(outDir, $"{ticker.ToLowerInvariant()}.csv");
                var fileExists = File.Exists(filePath);

                using var writer = new StreamWriter(filePath, append: true);
                if (!fileExists)
                {
                    // no header, Reader expects raw data
                }

                foreach (var l in lines)
                {
                    writer.WriteLine(l);
                }

                Console.WriteLine($"[BWPV] Wrote {lines.Count} rows for {ticker} into {filePath}");
            }
        }
    }
}
