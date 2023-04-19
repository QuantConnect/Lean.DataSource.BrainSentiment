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
using System.Linq;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using NodaTime;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Universe Selection helper class for BrainCompanyFilingLanguageMetrics dataset
    /// </summary>
    public class BrainCompanyFilingLanguageMetricsUniverse<T> : BaseData
        where T : BrainCompanyFilingLanguageMetricsUniverse<T>, new()
    {
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);
        
        /// <summary>
        /// Language Metric score by report part
        /// </summary>
        public BrainCompanyFilingLanguageMetrics ReportSentiment { get; set; }

        /// <summary>
        /// Language Metric score by risk factor statement part
        /// </summary>
        public BrainCompanyFilingLanguageMetrics RiskFactorsStatementSentiment { get; set; }

        /// <summary>
        /// Language Metric score by Management Discussion Analyasis Of Financial Condition And Results Of Operations
        /// </summary>
        public BrainCompanyFilingLanguageMetrics ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations { get; set; }

        /// <summary>
        /// Report Type of which the language metric came from
        /// </summary>
        protected virtual string ReportType { get; set; }

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

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
                    $"report_{ReportType.ToLowerInvariant()}",
                    "universe",
                    $"{date:yyyyMMdd}.csv"
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
            if (string.IsNullOrWhiteSpace(line))
            {
                return null;
            }

            var csv = line.Split(',').ToList();

            var data = (BrainCompanyFilingLanguageMetricsUniverse<T>)((object)new T());

            data.ReportSentiment = BrainCompanyFilingLanguageMetrics.Parse(csv.Skip(2).Take(11).ToList());
            data.RiskFactorsStatementSentiment = BrainCompanyFilingLanguageMetrics.Parse(csv.Skip(13).Take(11).ToList());
            data.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations = BrainCompanyFilingLanguageMetrics.Parse(csv.Skip(24).Take(11).ToList());

            data.Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]);
            // subtract 12 hours to match the base type data time
            data.Time = date.AddHours(-12);
            data.Value = csv[4].IfNotNullOrEmpty(0m, s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture));

            return data;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $@"{Symbol},
                {ReportSentiment.Sentiment},
                {RiskFactorsStatementSentiment.Uncertainty},
                {ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.Litigious},
                {ReportSentiment.Constraining},
                {RiskFactorsStatementSentiment.Interesting},
                {ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.Readability},
                {ReportSentiment.LexicalRichness},
                {RiskFactorsStatementSentiment.LexicalDensity},
                {ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.SpecificDensity},
                {ReportSentiment.SentenceCount},
                {RiskFactorsStatementSentiment.MeanSentenceLength}";
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
            return TimeZones.Utc;
        }
    }
}