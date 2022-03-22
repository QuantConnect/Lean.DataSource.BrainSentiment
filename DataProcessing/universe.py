from datetime import datetime
import os
from pathlib import Path
from CLRImports import *

class UniverseDataProcessing:
    def __init__(self, map_file_provider, path=None):
        self.map_file_provider = map_file_provider
        self.path = path if path else Path(Globals.DataFolder) / "alternative" / "brain"
                    
    def rank_universe_creation(self):
        data, universe_path = self.universe_creation("rankings")

        for date, ticker_data in data.items():
            for i, (ticker, datum) in enumerate(ticker_data.items()):
                date_time = datetime.strptime(date, "%Y%m%d")
                sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, True, self.map_file_provider, date_time)
                mode = "a" if i != 0 else "w"

                with open(f"{universe_path}/{date}.csv", mode, encoding="utf-8") as csv:
                    csv.write(f"{sid},{ticker.upper()},{datum['2']},{datum['3']},{datum['5']},{datum['10']},{datum['21']}\n")

    def report_universe_creation(self, data, universe_path):
        for date, ticker_data in data.items():
            for i, (ticker, datum) in enumerate(ticker_data.items()):
                date_time = datetime.strptime(date, "%Y%m%d")
                sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, True, self.map_file_provider, date_time)
                mode = "a" if i != 0 else "w"

                with open(f"{universe_path}/{date}.csv", mode, encoding="utf-8") as csv:
                    csv.write(f"{sid},{ticker.upper()},{datum}\n")

    def report_10k_universe_creation(self):
        data, universe_path = self.universe_creation("report_10k", report=True)
        self.report_universe_creation(data, universe_path)

    def report_all_universe_creation(self):
        data, universe_path = self.universe_creation("report_all", report=True)
        self.report_universe_creation(data, universe_path)

    def sentiment_universe_creation(self):
        data, universe_path = self.universe_creation("sentiment")

        for date, ticker_data in data.items():
            for i, (ticker, datum) in enumerate(ticker_data.items()):
                date_time = datetime.strptime(date, "%Y%m%d")
                sid = SecurityIdentifier.GenerateEquity(ticker, Market.USA, True, self.map_file_provider, date_time)
                mode = "a" if i != 0 else "w"

                with open(f"{universe_path}/{date}.csv", mode, encoding="utf-8") as csv:
                    csv.write(f"{sid},{ticker.upper()},{datum['7']},{datum['30']}\n")

    def universe_creation(self, dataset, report=False):
        base_path = self.path / dataset
        universe_path = base_path / "universe"
        Path.mkdir(universe_path, parents=True, exist_ok=True)

        paths = []
        data = {}

        for path, subdirs, files in os.walk(base_path):
            for name in files:
                paths.append(os.path.join(path, name))

        for file in paths:
            if "universe" in file: continue

            ticker = file.split("\\")[-1].split(".")[0]
            days = file.split("\\")[-3]

            with open(file, "r", encoding="utf-8") as csv:
                for line in csv.readlines():
                    datum = line.split(",")
                    date = datum[0]

                    if date not in data:
                        data[date] = {}

                    if ticker not in data[date]:
                        data[date][ticker] = {}

                    if not report:
                        data[date][ticker][days] = ",".join(datum[1:]).replace("\n", "")
                    else:
                        data[date][ticker] = ",".join(datum[3:36]).replace("\n", "")

        return data, universe_path