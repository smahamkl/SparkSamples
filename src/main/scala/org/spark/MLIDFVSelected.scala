package org.spark

import sorting.MLIDSecSort
/**
  * Created by bbejeck on 7/31/15.
  */
object MLIDFVSelected {

  def main(args: Array[String]) = {

    //Grouping.runGroupingExample(args)
    //AggregateByKey.runAggregateByKeyExample()
    //CombineByKey.runCombineByKeyExample()
    //StripFirstLines.stripLinesExample(args)
    //Splitting.runSplitExample()
    // MappingValues.runMappingValues()
    //AirlineFlightPerformance.runInitialFlightPerformanceDataFrame(args(0))

    MLIDSecSort.runSecondarySortExample(args)


  }

}
