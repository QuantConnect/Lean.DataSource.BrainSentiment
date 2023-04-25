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

using System.Collections.Generic;
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;
using QuantConnect.Interfaces;

namespace QuantConnect.Algorithm.CSharp
{
    public class BrainSentimentRegressionAlgorithm : QCAlgorithm, IRegressionAlgorithmDefinition
    {
        public Symbol _aapl, _customDataSymbol;

        public override void Initialize()
        {
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

            SetStartDate(2021, 2, 1);
            SetEndDate(2021, 2, 10);
            SetCash(100000);

            _aapl = AddEquity("AAPL").Symbol;
            _customDataSymbol = AddData<BrainSentimentIndicator7Day>(_aapl).Symbol;

            // add a custom universe data source (defaults to usa-equity)
            AddUniverse<BrainSentimentIndicatorUniverse>("BrainSentimentIndicatorUniverse", Resolution.Daily, (data) => {
                var aapl = data.Where(d => d.Symbol == _aapl).First();
                var sentiment = aapl.Sentiment7Days;
                Debug($"{Time}:: received universe selection data:: {sentiment}");
                return new List<Symbol> { aapl.Symbol };
            });
        }

        public override void OnData(Slice slice)
        {
            var data = slice.Get<BrainSentimentIndicator7Day>();
            if (data.ContainsKey(_customDataSymbol))
            {
                var sentiment = data[_customDataSymbol].Sentiment;
                Debug($"{Time}:: received sentiment base data:: {sentiment}");
            }
        }

        /// <summary>
        /// This is used by the regression test system to indicate if the open source Lean repository has the required data to run this algorithm.
        /// </summary>
        public bool CanRunLocally { get; } = true;

        /// <summary>
        /// This is used by the regression test system to indicate which languages this algorithm is written in.
        /// </summary>
        public Language[] Languages { get; } = { Language.CSharp, Language.Python };

        /// <summary>
        /// Data Points count of all timeslices of algorithm
        /// </summary>
        public long DataPoints => 2885;

        /// <summary>
        /// Data Points count of the algorithm history
        /// </summary>
        public int AlgorithmHistoryDataPoints => 0;

        /// <summary>
        /// This is used by the regression test system to indicate what the expected statistics are from running the algorithm
        /// </summary>
        public Dictionary<string, string> ExpectedStatistics => new()
        {
            { "Total Trades", "0" },
            { "Average Win", "0%" },
            { "Average Loss", "0%" },
            { "Compounding Annual Return", "0%" },
            { "Drawdown", "0%" },
            { "Expectancy", "0" },
            { "Net Profit", "0%" },
            { "Sharpe Ratio", "0" },
            { "Probabilistic Sharpe Ratio", "0%" },
            { "Loss Rate", "0%" },
            { "Win Rate", "0%" },
            { "Profit-Loss Ratio", "0" },
            { "Alpha", "0" },
            { "Beta", "0" },
            { "Annual Standard Deviation", "0" },
            { "Annual Variance", "0" },
            { "Information Ratio", "-19.8" },
            { "Tracking Error", "0.089" },
            { "Treynor Ratio", "0" },
            { "Total Fees", "$0.00" },
            { "Estimated Strategy Capacity", "$0" },
            { "Lowest Capacity Asset", "" },
            { "Portfolio Turnover", "0%" },
            { "OrderListHash", "d41d8cd98f00b204e9800998ecf8427e" }
        };
    }
}