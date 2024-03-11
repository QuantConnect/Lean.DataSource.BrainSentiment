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
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;

namespace QuantConnect.Algorithm.CSharp
{
    public class BrainSentimentIndicatorUniverseAlgorithm : QCAlgorithm
    {
        public override void Initialize()
        {
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

            SetStartDate(2021, 2, 14);
            SetEndDate(2021, 2, 18);
            SetCash(100000);

            // add a custom universe data source (defaults to usa-equity)
            var universe = AddUniverse<BrainSentimentIndicatorUniverse>(data =>
            {
                foreach (BrainSentimentIndicatorUniverse datum in data)
                {
                    Log($@"{datum.Symbol},
                    {datum.TotalArticleMentions7Days},
                    {datum.SentimentalArticleMentions7Days},
                    {datum.Sentiment7Days},
                    {datum.TotalBuzzVolume7Days},
                    {datum.SentimentalBuzzVolume7Days},
                    {datum.TotalArticleMentions30Days},
                    {datum.SentimentalArticleMentions30Days},
                    {datum.Sentiment30Days},
                    {datum.TotalBuzzVolume30Days},
                    {datum.SentimentalBuzzVolume30Days}");
                }

                // define our selection criteria
                return from BrainSentimentIndicatorUniverse d in data
                       where d.TotalArticleMentions7Days > 0m && d.Sentiment7Days > 0m
                       select d.Symbol;
            });

            var history = History(universe, 1).ToList();
            if (history.Count != 1)
            {
                throw new System.Exception($"Unexpected historical data count!");
            }
            foreach (var dataForDate in history)
            {
                var coarseData = dataForDate.ToList();
                if (coarseData.Count < 1000)
                {
                    throw new System.Exception($"Unexpected historical universe data!");
                }
            }
        }
        
        public override void OnSecuritiesChanged(SecurityChanges changes)
        {
            Log(changes.ToString());
        }
    }
}