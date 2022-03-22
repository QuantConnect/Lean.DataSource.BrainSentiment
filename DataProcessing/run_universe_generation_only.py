from CLRImports import *
from universe import UniverseDataProcessing

if __name__ == '__main__':
    map_file_provider = LocalZipMapFileProvider()
    map_file_provider.Initialize(DefaultDataProvider())

    universe_processor = UniverseDataProcessing(map_file_provider)
    universe_processor.report_10k_universe_creation()
    universe_processor.report_all_universe_creation()
    universe_processor.rank_universe_creation()
    universe_processor.sentiment_universe_creation()