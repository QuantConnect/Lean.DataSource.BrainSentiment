# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *

class BrainSentimentIndicatorAlgorithm(QCAlgorithm):
    def Initialize(self):
        self.SetStartDate(2020, 10, 07)  #Set Start Date
        self.SetEndDate(2020, 10, 11)    #Set End Date
        self.equity_symbol = self.AddEquity("AAPL", Resolution.Daily).Symbol
        self.custom_data_symbol = self.AddData(BrainSentimentIndicator7Day, self.equity_symbol).Symbol

    def OnData(self, slice):
        data = slice.Get(BrainSentimentIndicator7Day)
        if data:
            sentiment = data[self.custom_data_symbol].Sentiment
            self.Log(f"{self.equity_symbol.Value}: {sentiment}")