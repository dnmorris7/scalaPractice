// EXERCISE
// Write some code that prints out the first 10 values of the Fibonacci sequence.
// This is the sequence where every number is the sum of the two numbers before it.
// So, the result should be 0, 1, 1, 2, 3, 5, 8, 13, 21, 34

def fibonacci(n:Int): Int = {
  n match {
    case 0 => return 0
    case 1 => return 1
    case _ => return fibonacci(n - 1) + fibonacci(n - 2)
  }
}

def fibonacciStepped(n:Int, m:Int, count:Int): Int = {

  count match {

    case 0 => return m
    case _ => return fibonacciStepped(m, m+n, count-1)
  }
}

def fibonacciFast(n:Int): Int = {
  n match {
    case 0 => return 0
    case 1 => return 1
    case _ => return fibonacciStepped(0,1,n-1)
  }
}


def fibonacciList(n:Int): Unit = {
  var list=(0 to n).map(fibonacci).toList
  list.foreach(println)
}

def fibonacciFastList(n:Int): Unit = {
  var list=(0 to n).map(fibonacciFast).toList
  list.foreach(println)
}

println("Fibonacci Sequence: " + fibonacci(0))

println("Fibbonaci Stepped: " +fibonacciStepped(0, 1, 8))

val time0=System.currentTimeMillis()

fibonacciList(43)
val time1=System.currentTimeMillis()

print("First Algorithm Completion Time: " + {time1-time0})
val time2=System.currentTimeMillis()
fibonacciFastList(43)
val time3=System.currentTimeMillis()
print("Second Algorithm Completion Time: " + {time3-time2})