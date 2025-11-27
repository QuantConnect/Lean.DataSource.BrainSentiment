using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Converts daily S3 BLMECT metrics + differences files into Lean-ready data:
    ///     alternative/brain/blmect/{symbol}.csv
    /// </summary>
    public class BrainLanguageMetricsEarningsCallsConverter
    {
        private readonly IAmazonS3 _s3;
        private readonly string _bucket;
        private readonly string _outputRoot;

        public BrainLanguageMetricsEarningsCallsConverter(string bucket, string outputRoot)
        {
            _bucket = bucket;
            _outputRoot = outputRoot;
            _s3 = new AmazonS3Client(RegionEndpoint.USEast1);
        }

        public void ProcessDate(DateTime date)
        {
            string fileDate = date.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

            // ============================================================================
            // 1. LOAD DIFF FILE
            // ============================================================================
            string diffKey = $"BLMECT/differences_earnings_call_{fileDate}.csv";
            Console.WriteLine($"[BLMECT] Downloading DIFF: {diffKey}");

            var diffByTicker = new Dictionary<string, string[]>(); // key = ticker

            try
            {
                using var diffResponse = _s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = diffKey
                }).Result;

                using var diffReader = new StreamReader(diffResponse.ResponseStream);

                string diffHeader = diffReader.ReadLine();
                char diffDelimiter = diffHeader.Contains('\t') ? '\t' : ',';

                string diffLine;
                while ((diffLine = diffReader.ReadLine()) != null)
                {
                    var parts = diffLine.Split(diffDelimiter);
                    if (parts.Length < 47)
                    {
                        Console.WriteLine($"[BLMECT] DIFF skipped row (too few columns): {parts.Length}");
                        continue;
                    }

                    string ticker = parts[1];
                    if (string.IsNullOrWhiteSpace(ticker))
                        continue;

                    diffByTicker[ticker] = parts;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[BLMECT] WARNING: DIFF file not found for {fileDate}. Continuing without diff. Error: {ex.Message}");
            }

            // ============================================================================
            // 2. LOAD METRICS FILE
            // ============================================================================
            string metricsKey = $"BLMECT/metrics_earnings_call_{fileDate}.csv";
            Console.WriteLine($"[BLMECT] Downloading METRICS: {metricsKey}");

            var rowsBySymbol = new Dictionary<string, List<string>>();

            using var metricsResponse = _s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = _bucket,
                Key = metricsKey
            }).Result;

            using var metricsReader = new StreamReader(metricsResponse.ResponseStream);

            string metricsHeader = metricsReader.ReadLine();
            char metricsDelimiter = metricsHeader.Contains('\t') ? '\t' : ',';

            string metricsLine;
            while ((metricsLine = metricsReader.ReadLine()) != null)
            {
                var parts = metricsLine.Split(metricsDelimiter);
                if (parts.Length < 29)
                {
                    Console.WriteLine($"[BLMECT] METRICS skipped row (too few columns): {parts.Length}");
                    continue;
                }

                string ticker = parts[1];
                if (string.IsNullOrWhiteSpace(ticker))
                    continue;

                diffByTicker.TryGetValue(ticker, out var diffRow);

                string output = BuildOutputRow(fileDate, parts, diffRow);

                if (!rowsBySymbol.ContainsKey(ticker))
                    rowsBySymbol[ticker] = new List<string>();

                rowsBySymbol[ticker].Add(output);
            }

            string outDir = Path.Combine(_outputRoot, "alternative", "brain", "blmect");
            Directory.CreateDirectory(outDir);

            foreach (var kvp in rowsBySymbol)
            {
                string ticker = kvp.Key.ToLowerInvariant();
                string filePath = Path.Combine(outDir, $"{ticker}.csv");

                using var writer = new StreamWriter(filePath, append: true);
                foreach (var line in kvp.Value)
                    writer.WriteLine(line);
            }

            Console.WriteLine($"[BLMECT] Finished {fileDate}: Wrote {rowsBySymbol.Count} symbols.");
        }


        private static string BuildOutputRow(string fileDate, string[] metrics, string[] diff)
        {
            var f = new List<string>();

            // 0) Snapshot date = file date
            f.Add(fileDate);

            // === METRICS SECTION ===
            // (We keep Fields 3–28 from metrics dataset)
            // Since metrics[0]=FIGI, metrics[1]=TICKER, metrics[2]=LAST_TRANSCRIPT_DATE

            f.Add(DateTime.TryParse(metrics[3], out var x) ? x.ToString("yyyyMMdd") : "");
            f.Add(metrics[4]);  // quarter
            f.Add(metrics[5]);  // year

            // MD metrics (indices 6..14)
            for (int i = 6; i < 15; i++)
                f.Add(metrics[i]);

            // AQ metrics (indices 15..19)
            for (int i = 15; i < 20; i++)
                f.Add(metrics[i]);

            // MA metrics (indices 20..28)
            for (int i = 20; i < 29; i++)
                f.Add(metrics[i]);

            // === DIFF SECTION ===
            if (diff != null)
            {
                // prev transcript info (indices 6,7,8)
                //f.Add(diff[6]); // prev date
                f.Add(DateTime.TryParse(diff[6], out var dtPrev) ? dtPrev.ToString("yyyyMMdd") : "");
                f.Add(diff[7]); // prev quarter
                f.Add(diff[8]); // prev year

                // MD deltas/similarities (indices 9..23)
                for (int i = 9; i <= 23; i++)
                    f.Add(diff[i]);

                // AQ deltas/similarities (indices 24..31)
                for (int i = 24; i <= 31; i++)
                    f.Add(diff[i]);

                // MA deltas/similarities (indices 32..46)
                for (int i = 32; i <= 46; i++)
                    f.Add(diff[i]);
            }
            else
            {
                // No diff row → pad blanks (41 fields)
                int blanks = 3 + 15 + 8 + 15; // = 41
                for (int i = 0; i < blanks; i++)
                    f.Add("");
            }

            return string.Join(",", f);
        }
    }
}
