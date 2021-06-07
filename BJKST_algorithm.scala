import scala.collection._
import scala.util.hashing.MurmurHash3
import util.control.Breaks._

class Book(val attribute: String){
    // println("Book class " + attribute)
    def create(): String = {
        return "book" + attribute
    }
}

object BJKST{

    def hashing(token: String, m: Int, iter: Int): Int = {
        var hashCode: Int = token.hashCode()
        hashCode = hashCode + (m+1-iter)
        // print("\n\thashing " + hashCode)
        return hashCode
    }

    def BJKST[A](stream: Stream[A], b: Double, c: Double, epsilon: Double){        
        stream.toSet
        var B: Map[Int, Int] = Map()
        var z: Int = 0
        var hHashLen: Int = stream.length
        var gHashLen: Int = (b*(1/(epsilon*epsilon*epsilon*epsilon))*((scala.math.log(stream.size))*(scala.math.log(stream.size)))).toInt
        var hHashSeed: Int = 1
        var gHashSeed: Int = 2

        for(token <- stream){       
            var hHashValue: Int = MurmurHash3.stringHash(token.toString, hHashSeed)
            hHashValue = hHashValue.abs
            hHashValue = hHashValue % (hHashLen + 1)
            var hHashValueBinary = hHashValue.toBinaryString
            var hHashValueBinaryReverse = hHashValueBinary.reverse
            var zeros: Int = 0    
            if (hHashValueBinaryReverse(0) == '0'){
                breakable{
                    for (bit <- hHashValueBinaryReverse){
                        if (bit == '0'){
                            zeros = zeros + 1
                        }else{
                            break
                        }
                    }
                }
            }
            // print("\n", token, "\t", hHashValue, "\t", hHashValueBinary, "\t", hHashValueBinaryReverse, "\t", zeros)
            if (zeros >= z){
                var gHashValue: Int = MurmurHash3.stringHash(token.toString, gHashSeed)
                gHashValue = gHashValue.abs
                gHashValue = gHashValue % (gHashLen + 1)
                B += (gHashValue -> zeros)

                if (B.size >= (c/(epsilon*epsilon)).toInt){
                    z = z + 1
                    B.keys.foreach{
                        i => {
                            if (B(i) <= z){
                                var tempB = B.-(i)
                                B = tempB
                            }
                        }
                    }
                }

            }
        }
        var bucketSize = (B.size)*(scala.math.pow(2,z))
        print("n = " + stream.size + ", b = " + b + ", c = " + c + ", epsilon = " + epsilon + "\nEstimated bucket size = " + bucketSize + "\n")
    }

    // Stream of objects + arguments needed by algorithms
    def StreamObjects[A](stream: Stream[A], b: Double, c: Double, epsilon: Double){
        var objectTypes: mutable.Map[Any, Int] = mutable.Map()
        var StreamToSet = stream.toSet
        var typesCounter = 0
        for(i<-StreamToSet){
            if(!objectTypes.contains(i.getClass)){
            objectTypes += (i.getClass -> typesCounter)
            typesCounter += 1
            }      
        }

        var objectSubStream: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer()

        for(j<-0 to typesCounter - 1){
            for(i<-stream){
                if(j == objectTypes(i.getClass)){
                    objectSubStream.append(i)
                }
            }
            println("\n" + objectSubStream)
            BJKST(objectSubStream.toStream, b, c, epsilon)
            objectSubStream.clear()
        }
    }    

    def main(items: Array[String]): Unit={

        var b: Double = 10.11 
        var c: Double = 3.12
        var epsilon: Double = 0.5
        // var b1 = new Book("1")
        // var aList = List(1,2,4,3,4,3,4,5,4,6, "a", "b", "c", b1, b1)
        var aList = List(1,2,4,3,4,3,4,5,4,6)
        val aListStream = aList.toStream
        StreamObjects(stream=aListStream, b=b, c=c, epsilon=epsilon)
    }
}