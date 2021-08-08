import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val text: DataSet[String] = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?"
    )

    val counts: AggregateDataSet[(String, Int)] = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}
