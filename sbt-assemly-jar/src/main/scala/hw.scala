import com.typesafe.scalalogging._
import org.slf4j.LoggerFactory

object Hi { 
    def main(args: Array[String]) = {
        val logger = Logger(LoggerFactory.getLogger("MyClass"))
        println("Hi! Fat JAR 3.")
        logger.info("Hello Logger Message")
    }
}

