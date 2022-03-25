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
using System.Collections.Generic;
using QuantConnect;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    public class BrainCompanyFilingLanguageMetrics
    {
        public int? SentenceCount { get; set; }

        public decimal? MeanSentenceLength { get; set; }

        public decimal? Sentiment { get; set; }

        public decimal? Uncertainty { get; set; }

        public decimal? Litigious { get; set; }

        public decimal? Constraining { get; set; }

        public decimal? Interesting { get; set; }

        public decimal? Readability { get; set; }

        public decimal? LexicalRichness { get; set; }

        public decimal? LexicalDensity { get; set; }

        public decimal? SpecificDensity { get; set; }

        public BrainCompanyFilingLanguageMetricsSimilarityDifference? Similarity { get; set; }

        public static BrainCompanyFilingLanguageMetrics Parse(List<string> metrics, List<string> similarity = null)
        {
            return new BrainCompanyFilingLanguageMetrics
            {
                SentenceCount = !string.IsNullOrWhiteSpace(metrics[0]) ? (int)QuantConnect.Parse.Decimal(metrics[0]) : null,
                MeanSentenceLength = !string.IsNullOrWhiteSpace(metrics[1]) ? QuantConnect.Parse.Decimal(metrics[1]) : null,
                Sentiment = !string.IsNullOrWhiteSpace(metrics[2]) ? QuantConnect.Parse.Decimal(metrics[2]) : null,
                Uncertainty = !string.IsNullOrWhiteSpace(metrics[3]) ? QuantConnect.Parse.Decimal(metrics[3]) : null,
                Litigious = !string.IsNullOrWhiteSpace(metrics[4]) ? QuantConnect.Parse.Decimal(metrics[4]) : null,
                Constraining = !string.IsNullOrWhiteSpace(metrics[5]) ? QuantConnect.Parse.Decimal(metrics[5]) : null,
                Interesting = !string.IsNullOrWhiteSpace(metrics[6]) ? QuantConnect.Parse.Decimal(metrics[6]) : null,
                Readability = !string.IsNullOrWhiteSpace(metrics[7]) ? QuantConnect.Parse.Decimal(metrics[7]) : null,
                LexicalRichness = !string.IsNullOrWhiteSpace(metrics[8]) ? QuantConnect.Parse.Decimal(metrics[8]) : null,
                LexicalDensity = !string.IsNullOrWhiteSpace(metrics[9]) ? QuantConnect.Parse.Decimal(metrics[9]) : null,
                SpecificDensity = !string.IsNullOrWhiteSpace(metrics[10]) ? QuantConnect.Parse.Decimal(metrics[10]) : null,

                Similarity = similarity == null ?
                    null :
                    BrainCompanyFilingLanguageMetricsSimilarityDifference.Parse(similarity)
            };
        }
    }
}