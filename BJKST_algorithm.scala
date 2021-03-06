import scala.collection._
import scala.util.hashing.MurmurHash3
import util.control.Breaks._
import java.io._

// sample class declaration becuase the algorithm below works for any object type
class Book(val attribute: String){
    def create(): String = {
        return "book" + attribute
    }
}

object BJKST{

    def BJKST[A](stream: Stream[A], b: Double, c: Double, epsilon: Double): Double ={        
        var B: Map[Int, Int] = Map() //map to hold our pairs key (hash function), number of zeroes at the end of binary representation
        var z: Int = 0 //minimal number of zeroes in the bucket
        var hHashLen: Int = stream.length //hash function h mapping n->n for modulo
        var gHashLen: Int = (b*(1/(epsilon*epsilon*epsilon*epsilon))*((scala.math.log(stream.size))*(scala.math.log(stream.size)))).toInt //for modulo
        var hHashSeed: Int = 1 //seed for first hash functon
        var gHashSeed: Int = 2 //seed for 2nd hash function
        var g: Int = 0
        var h: Int = 0

        for(token <- stream){    //going through every item in stream   
            var hHashValue: Int = MurmurHash3.stringHash(token.toString, hHashSeed) //calculating the value of h with murmur3
            hHashValue = hHashValue.abs //taking absolute value
            hHashValue = hHashValue % (hHashLen + 1) //modulo

            var hHashValueBinary = hHashValue.toBinaryString //turning h into binary string
            var hHashValueBinaryReverse = hHashValueBinary.reverse //reversing it so 0's at the start
            var zeros: Int = 0    //number of zeroes in current item

            if (hHashValueBinaryReverse(0) == '0'){ //if first bit is 0
                breakable{
                    for (bit <- hHashValueBinaryReverse){ //going through every bit
                        if (bit == '0'){ //if bit is 0 increase enumerator by 1
                            zeros = zeros + 1
                        }else{ //otherwise break the loop for counting
                            break
                        }
                    }
                }
            }

            if (zeros >= z){ //if the number of zeroes in our item is greater than minimal number of zeroes to get into bucket we add it to the bucket
                var gHashValue: Int = MurmurHash3.stringHash(token.toString, gHashSeed) //calculating g
                gHashValue = gHashValue.abs //absolute value of g
                gHashValue = gHashValue % (gHashLen + 1) //modulo

                B += (gHashValue -> zeros) //add key (g) to mapping with value (number of zeroes at the end of binary representation)

                if (B.size >= (c/(epsilon*epsilon)).toInt){ //if our map is of size c/e^2 we need to remove some elements
                    z = z + 1 //increase minimal value of zeroes to get into mapping
                    B.keys.foreach{ //go through every key in map
                        i => {
                            if (B(i) <= z){ //if the number of zeroes for the key is less than minimal value remove this pair key, value
                                var tempB = B.-(i)
                                B = tempB
                            }
                        }
                    }
                }
            }
        }

        var bucketSize = (B.size)*(scala.math.pow(2,z)) //estimate the number of unique elements in the stream to be size of bucket (size of map) * 2^(minimal value of zeroes to get into mapping)
        print("n = " + stream.size + ", b = " + b + ", c = " + c + ", epsilon = " + epsilon + "\nEstimated bucket size = " + bucketSize + "\n") //print our result
        return bucketSize
    }

    // Stream of objects + arguments needed by algorithms
    def StreamObjects[A](stream: Stream[A], b: Double, c: Double, epsilon: Double){
        var objectTypes: mutable.Map[Any, Int] = mutable.Map() //make a map to hold key (type of object), value (integer) to make a substream of objects of same type
        var StreamToSet = stream.toSet //turn stream to set to reduce the size
        var typesCounter = 0 //counter for assigning types to substreams
        for(i<-StreamToSet){ //go throught every item in the set
            if(!objectTypes.contains(i.getClass)){ //if our mapping does not contain this type of object add it to mapping
            objectTypes += (i.getClass -> typesCounter)
            typesCounter += 1 //increase counter
            }      
        }

        var objectSubStream: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer()

        for(j<-0 to typesCounter - 1){ //go through every type of object in the stream
            for(i<-stream){
                if(j == objectTypes(i.getClass)){ //if the current element is of this object type add it to substream
                    objectSubStream.append(i)
                }
            }
            BJKST(objectSubStream.toStream, b, c, epsilon) //call function with substream
            objectSubStream.clear() //clear the array so we can use it again for the next type of object
        }
    }    

    def main(items: Array[String]): Unit={

        var b: Double = 10.11 
        var c: Double = 3.12
        var epsilon: Double = 0.5

        // creating a sample array
        var newList: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer()
        for(i<-0 to 2000000){
            var r = scala.util.Random
            newList.append(r.nextInt(1000))
        }
        println("Unique elements in list: " + newList.toSet.size)

        val pw = new PrintWriter(new File("Epsilon.txt")) //saving to file

        for(i<-1 to 50){ //loop to change epsilon
            var Out = BJKST(stream=newList.toStream, b=b, c=c, epsilon=(1.0 / i)) //calculate estimated unique elements
            pw.write((1.0 / i) + "\t" + Out + "\n")
        }
        pw.close
        
        // the algorithm works for any object type; below you can instiantiate i.e. a book object and add it to a stream
        // var b1 = new Book("1")
        // var aList = List(1,2,4,3,4,3,4,5,4,6, "a", "b", "c", b1, b1)
        //val aListStream = aList.toStream
        //StreamObjects(stream=aListStream, b=b, c=c, epsilon=epsilon)
    }
}