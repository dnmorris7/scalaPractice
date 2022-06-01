import shapeless.syntax.std.tuple.productTupleOps
// Flow control

// If / else:
if (1 > 3) println("Impossible!") else println("The world makes sense.")

if (1 > 3) {
  println("Impossible!")
  println("Really?")
} else {
  println("The world makes sense.")
  println("still.")
}

// Matching
val number = 2
number match {
  case 1 => println("One")
  case 2 => println("Two")
  case 3 => println("Three")
  case _ => println("Something else")
}

for (x <- 1 to 4) {
  val squared = x * x
  println(squared)
}

var x = 10
while (x >= 0) {
  println(x)
  x -= 1
}

x = 0
do { println(x); x+=1 } while (x <= 10)

// Expressions

{val x = 10; x + 20}

println({val x = 10; x + 20})

var array = Array( 1, 2, 3 )

println(array)
array = array :+ 4
array = array :+ 5

println(array)
for (x <- array )
{
  print( x )
}

//val vs var - val is IMMUTABLE, so sequence won't be changed (easily at least); you'll need to make a new val off it.
//var is MUTABLE -
val sequence=(0, 1)
var array = Array(0,1)
sequence :+ 2
println(sequence._1)

array=array :+ 2
println("Array Value: " +array.toList)
array.foreach(println)


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
def fibonacciList(n:Int): Unit = {
  var list=(0 to n).map(fibonacci).toList
  list.foreach(println)
}

println("Fibonacci Sequence: " + fibonacci(0))

def fibonacciStepped(n:Int, m:Int, count:Int): Int = {

  count match {

    case 0 => return m
    case _ => return fibonacciStepped(m, m+n, count-1)
  }
}
println("Fibbonaci Stepped: " +fibonacciStepped(0, 1, 8))



def fibonacciFast(n:Int): Int = {
  n match {
    case 0 => return 0
    case 1 => return 1
    case _ => return fibonacciStepped(0,1,n-2)
  }
}

fibonacciList(10)


//def fibonacciList(n:Int): Array[Int] = {
//  var list=(1 to n).toList
//
//
//
//
//  return list
//
//}


//
//  if(n<-0){
//    return 0;
//  }
//  else{
//    return 1;
//  }
//}
//
//for (i <-1 to 10)
//sequence :+ sequence._1 + sequence._2