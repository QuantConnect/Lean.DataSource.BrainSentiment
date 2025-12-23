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

using Amazon;
using Amazon.S3;
using QuantConnect.Util;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace QuantConnect.DataProcessing
{
    /// <summary>
    /// Base implementation for Brain dataset converters.
    /// </summary>
    public class BrainDataConverter : IDisposable
    {
        protected AmazonS3Client S3Client { get; }
        protected string BucketName { get; }
        protected string Prefix { get; }

        private readonly string _processedDataDirectory;
        private readonly string _destinationDirectory;

        public BrainDataConverter(string prefix, string outputRoot)
        {
            BucketName = Environment.GetEnvironmentVariable("BRAIN_S3_BUCKET_NAME");
            var awsAccessKeyId = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY_ID");
            var awsSecretAccessKey = Environment.GetEnvironmentVariable("AWS_SECRET_ACCESS_KEY");

            if (string.IsNullOrWhiteSpace(BucketName) ||
                string.IsNullOrWhiteSpace(awsAccessKeyId) ||
                string.IsNullOrWhiteSpace(awsSecretAccessKey))
            {
                throw new ArgumentNullException("The BRAIN_S3_BUCKET_NAME, AWS_ACCESS_KEY_ID, or AWS_SECRET_ACCESS_KEY environment variables is not set.");
            }

            S3Client = new AmazonS3Client(
                awsAccessKeyId,
                awsSecretAccessKey,
                RegionEndpoint.USEast1
            );

            prefix = prefix.Trim().ToLowerInvariant();
            Prefix = prefix.ToUpperInvariant();

            _processedDataDirectory = Path.Combine(Globals.DataFolder, "alternative", "brain", prefix);
            _destinationDirectory = Path.Combine(outputRoot, prefix);
            Directory.CreateDirectory(_destinationDirectory);
        }

        /// <summary>
        /// Converts all available deployment dates.
        /// </summary>
        public virtual bool ProcessHistory() => throw new NotImplementedException();
        
        /// <summary>
        /// Processes a single deployment date file.
        /// </summary>
        public virtual bool ProcessDate(DateTime date) => throw new NotImplementedException();

        public void SaveContentToFile(string ticker, IEnumerable<string> contents)
        {
            ticker = ticker.ToLowerInvariant();
            var filePath = Path.Combine(_processedDataDirectory, $"{ticker}.csv");
            var finalPath = Path.Combine(_destinationDirectory, $"{ticker}.csv");

            var finalFileExists = File.Exists(filePath);
            if (!finalFileExists)
            {
                filePath = finalPath;
                finalFileExists = File.Exists(filePath);
            }

            var lines = new HashSet<string>(contents);
            if (finalFileExists)
            {
                foreach (var line in File.ReadAllLines(filePath))
                {
                    lines.Add(line);
                }
            }

            File.WriteAllLines(finalPath, [.. lines.OrderBy(x => x[0..8])]);
        }

        /// <summary>
        /// Disposes the S3 client.
        /// </summary>
        public void Dispose() => S3Client?.DisposeSafely();
    }
}