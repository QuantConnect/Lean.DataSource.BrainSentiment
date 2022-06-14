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
 *
*/

using System;
using NodaTime;
using System.IO;
using QuantConnect;
using QuantConnect.Data;
using System.Collections.Generic;
using System.Linq;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Brain sentiment on news
    /// </summary>
    public class BrainSentimentIndicatorBase<T> : BaseData
        where T : BrainSentimentIndicatorBase<T>, new()
    {
        public int TotalArticleMentions { get; set; }

        public decimal SentimentalArticleMentions { get; set; }

        public decimal Sentiment { get; set; }

        public decimal? TotalBuzzVolume { get; set; }

        public decimal? SentimentalBuzzVolume { get; set; }

        protected virtual int LookbackDays { get; set; }

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "brain",
                    "sentiment",
                    $"{LookbackDays}",
                    $"{date:yyyyMM}",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',').ToList();
            var dataDate = csv[0];
            csv = csv.Skip(1).ToList();

            var data = (BrainSentimentIndicatorBase<T>)((object)new T());

            data.TotalArticleMentions = Parse.Int(csv[0]);
            data.SentimentalArticleMentions = Parse.Int(csv[1]);
            data.Sentiment = Parse.Decimal(csv[2]);

            data.TotalBuzzVolume = !string.IsNullOrWhiteSpace(csv[3])
                ? Parse.Decimal(csv[3])
                : null;
            data.SentimentalBuzzVolume = !string.IsNullOrWhiteSpace(csv[4])
                ? Parse.Decimal(csv[4])
                : null;

            data.Symbol = config.Symbol;
            data.EndTime = QuantConnect.Parse.DateTimeExact(dataDate, "yyyyMMdd").AddHours(12);

            return data;
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        protected T CloneData()
        {
            return new T
            {
                TotalArticleMentions = TotalArticleMentions,
                SentimentalArticleMentions = SentimentalArticleMentions,
                Sentiment = Sentiment,

                TotalBuzzVolume = TotalBuzzVolume,
                SentimentalBuzzVolume = SentimentalBuzzVolume,

                Symbol = Symbol,
                EndTime = EndTime
            };
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol and requires that corporate events be applied to it as well, such as renames and delistings
        /// </summary>
        /// <returns>false</returns>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - Mentions: {TotalArticleMentions}, Sentiment: {Sentiment}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }
    }
}
