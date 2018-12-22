object HelloWorld {
  def main(args: Array[String]): Unit = {
    if(args.length == 0) {
      println("ARGS REQUIRED")
      return
    }
    println(args(0))
  }
}