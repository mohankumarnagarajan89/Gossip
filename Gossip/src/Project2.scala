
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.collection.mutable._
import scala.concurrent.duration._
import scala.util.Random

object Project2 extends App
 {
	var numNodes = args(0).toInt
	var topology = args(1)
	var algorithm = args(2)

	/*
	 * Create a master which takes the number of nodes, topology type and algorithm to be used
	 */
	val master = ActorSystem("MasterSystem").actorOf(Props(new Master(numNodes,topology,algorithm)),"master"); 		  		  		
  	master ! StartComputation
   	
}

class Master(numberOfNodes:Int,topology:String,algorithm:String) extends Actor
{
  /*
   * Define the neighbors for each node and form an neighbor set depending on the given topology
   */
	
	val workersList = new Array[ActorRef](numberOfNodes);
	var numNeighbours = 0
	var neighbours = new Array[Int](5)
    var numColums:Int =0
	var nodeVisitedCount = 0
    var nodeCompletedCount =0
    val system = ActorSystem("Workersystem")

    /*
     * Worker number list is used to identify the nodes available for propagation. Used for full topology instead of neighbor array 
     */
    var workerNumberList = new ArrayBuffer[Int]
    for(neighbourCount:Int <- 0 to numberOfNodes-1)
    {
      workerNumberList+=neighbourCount
    }
    
	/*
	 * This list keeps track of alive nodes in the list. Used by line, 2D and Imp 2D
	 */
    var nodesAlive = new Array[Int](numberOfNodes)
    for(nodeCount:Int <- 0 to numberOfNodes-1)
    {
      nodesAlive(nodeCount)=1
    }
    
    /*
     *    Create workers and form the neighbor list.
     *    Associate the neighbor list with the worker to send messages  
     */    
    
    for(nodeCount:Int <- 0 to numberOfNodes-1)
        {
      
    	if(topology.equalsIgnoreCase("line"))
    	{
    	  if(nodeCount == 0)
			{
				numNeighbours = 1;
				neighbours(0) = 1; 	
			}
			else if(nodeCount == numberOfNodes-1)
			{
				numNeighbours = 1;
				neighbours(0) = numberOfNodes-2; 	
			}
			else
			{
				numNeighbours =2;
				neighbours(0) = nodeCount-1;
				neighbours(1) = nodeCount+1; 	
			}
    	}
    	if(topology.equalsIgnoreCase("2D"))
    	{
    		numColums = math.ceil(math.sqrt(numberOfNodes)).toInt;
			numNeighbours=0;					
			if(nodeCount-1 >= 0)
			{
				neighbours(numNeighbours) = nodeCount-1;
				numNeighbours=numNeighbours+1
			}
			if(nodeCount-numColums >= 0)
			{
				neighbours(numNeighbours) = nodeCount-numColums;
				numNeighbours=numNeighbours+1
			}				
			if(nodeCount+numColums <= numberOfNodes-1)
			{
				neighbours(numNeighbours) = nodeCount+numColums;
				numNeighbours=numNeighbours+1
			}
			if(nodeCount+1 <= numberOfNodes-1)
			{
				neighbours(numNeighbours) = nodeCount+1;
				numNeighbours=numNeighbours+1
			}	
		
    	}
    	if(topology.equalsIgnoreCase("imp2D"))
    	{
    	  numColums = math.ceil(math.sqrt(numberOfNodes)).toInt;
			numNeighbours=0;					
			if(nodeCount-1 >= 0)
			{
				neighbours(numNeighbours) = nodeCount-1;
				numNeighbours=numNeighbours+1
			}
			if(nodeCount-numColums >= 0)
			{
				neighbours(numNeighbours) = nodeCount-numColums;
				numNeighbours=numNeighbours+1
			}				
			if(nodeCount+numColums <= numberOfNodes-1)
			{
				neighbours(numNeighbours) = nodeCount+numColums;
				numNeighbours=numNeighbours+1
			}
			if(nodeCount+1 <= numberOfNodes-1)
			{
				neighbours(numNeighbours) = nodeCount+1;
				numNeighbours=numNeighbours+1
			}
			neighbours(numNeighbours) = numberOfNodes-1-nodeCount;
			numNeighbours=numNeighbours+1
    		}
    	
       	workersList(nodeCount) = system.actorOf(Props(new Worker(nodeCount,topology,workersList,workerNumberList,neighbours,nodesAlive,self,system)));
        
        }
    
     	val startTime = System.currentTimeMillis()
    	println("\n\n\t Processing Started\n\n")
    
   def receive = {
      
    
    case StartComputation =>
      /*
       * Based on the given algorithm, perform the functionality
       * If algorithm = gossip, master sends the rumor to any node in the system and process continues.
       * If algorithm = pushsum, initial (s,w) pair is sent to a node and the computation starts.  
       */
          
      var nodeNumber = scala.util.Random.nextInt(numberOfNodes)
      if(algorithm.equalsIgnoreCase("gossip"))
        workersList(nodeNumber) ! SendMessage
       else
         workersList(nodeNumber) ! PushSumPair(nodeNumber,1)
         
    /*
     *     Case class which executes when the node is visited atleast once
     */     
    case NodeVisited =>
        nodeVisitedCount =nodeVisitedCount+1
       
        if(nodeVisitedCount  == numberOfNodes)
        {
          val endTime = System.currentTimeMillis()
          println("\n\n\t*******Time taken for convergence : " + (endTime-startTime) + " milliseconds*******\n\n")
        }
     
        /*
         *  Case class which is triggered when all the nodes in the system receives the rumour 10 times
         */
     case NodeCompleted =>
        nodeCompletedCount =nodeCompletedCount+1
        //println("Number of nodes completed receiving the rumor 10 times : " + nodeCompletedCount)
        if(nodeCompletedCount  == numberOfNodes-1)
        {
          println("***********All nodes completed**********")
          System.exit(1)
        }  
        
     /*
      * Case class which is used for convergence in pushsum
      */ 
     case PushSumNodeConverged(sumRatio) =>
         
	    	val endTime = System.currentTimeMillis()
	    	println("\n\n\t*******Time taken for convergence : " + (endTime-startTime) + " milliseconds*******\n\n")
	    	//println("Final push sum ratio : "+sumRatio)
	    	System.exit(1)
	   }
}

/*
 * Worker class which propagates the messages for gossip and pushsum algorithms
 */
class Worker(workerNumber:Int,topology:String,workerList:Array[ActorRef],workerNumberList:ArrayBuffer[Int],neighbours:Array[Int],nodesAlive:Array[Int],master: ActorRef,system:ActorSystem) extends Actor
{
  var visitedCount=0
  var isNodeCompleted = false
  var nextWorkerCount = 0
  var neighbour =0
  var numNeighbours=neighbours.length
  var numberOfNodes = workerList.length
  
  var sCurrentValue :Double = workerNumber
  var wCurrentValue :Double= 1
  var convergenceCount = 0
  var sIntermediate =0.0
  var wIntermediate =0.0
  var ratio = sCurrentValue/wCurrentValue
  
  def receive = {
    /*
     * This case class is used to send a message to other nodes
     */
    case SendMessage =>

      /*
       * Implementation logic:
       * 1. Check if the node is atleast visited once. If yes, send a message to master as visited.
       * 2. Check if the node is visited 20 times(we have taken 20 as the arbitrary number). If yes, send a completion message to master. Also remove the node from the list and reset the alive status of the node
       * 3. Based on the topology, form the neighbors list. Select a random neighbor and send a message. Use a scheduler to send a message after a time interval.
       * 4. Convergence time varies depending on the topology. Report has more details on the same.
       */
      
		    if(!isNodeCompleted )
		    {
		       if(visitedCount==0)
		       {
		    	   master ! NodeVisited
		       }
		       if(visitedCount ==20)
		       {
		        isNodeCompleted =true
		        workerNumberList.drop(workerNumber)
		        nodesAlive(workerNumber)=0
		        master ! NodeCompleted
		       }
		       
		       if(topology.equalsIgnoreCase("full"))
		       {
			  		nextWorkerCount =  scala.util.Random.nextInt(workerNumberList.length)
			    	if((workerNumberList.length==1) || (nextWorkerCount!=workerNumber))
			    	{
			    	  if(!sender.equals(self))
			    	  {
			    	   visitedCount =visitedCount+1
			    	  }
			    	   workerList(nextWorkerCount) ! SendMessage
			    	    
			    	   import system.dispatcher
			    	   val delayTime = scala.concurrent.duration.FiniteDuration(25, "milliseconds")
			    	   context.system.scheduler.scheduleOnce(delayTime, self, SendMessage)
			    	 }
		       }
		       
		       if(topology.equalsIgnoreCase("line"))
		       {
		          
		         if(workerNumber==0)
		         {
		           workerList(1) ! SendMessage
		         }
		         else if (workerNumber==workerList.length-1)
		         {
		           workerList(workerList.length-2) ! SendMessage
		         }
		         else
		         {
		            nextWorkerCount =  Random.nextInt(2)
		
		            if(nextWorkerCount==0)
		              neighbour = workerNumber+1
		            else
		              neighbour = workerNumber-1
		              
		            if((nodesAlive(neighbour)==1)&&(!isNodeCompleted))
		            {
		            	if(!sender.equals(self))
			    	  {
			    	   visitedCount =visitedCount+1
			    	  }
		              
		            	workerList(neighbour) ! SendMessage
		            }
		         }
		         
		         	import system.dispatcher
		         	val delayTime = scala.concurrent.duration.FiniteDuration(20, "milliseconds")
			   		context.system.scheduler.scheduleOnce(delayTime, self, SendMessage)
		        }
		       
		       if(topology.equalsIgnoreCase("2D"))
		       {
		    	   	var numColums:Int = math.ceil(math.sqrt(numberOfNodes)).toInt
					numNeighbours=0					
					if(workerNumber-1 >= 0)
					{
						neighbours(numNeighbours) = workerNumber-1
						numNeighbours=numNeighbours+1
					}
					if(workerNumber-numColums >= 0)
					{
						neighbours(numNeighbours) = workerNumber-numColums
						numNeighbours=numNeighbours+1
					}				
					if(workerNumber+numColums <= numberOfNodes-1)
					{
						neighbours(numNeighbours) = workerNumber+numColums
						numNeighbours=numNeighbours+1
					}
					if(workerNumber+1 <= numberOfNodes-1)
					{
						neighbours(numNeighbours) = workerNumber+1
						numNeighbours=numNeighbours+1
					}	
					
					nextWorkerCount =  Random.nextInt(numNeighbours)
					if((nodesAlive(neighbours(nextWorkerCount))==1)&&(!isNodeCompleted))
		            {
		            	 if(!sender.equals(self))
		            	 {
		            		 visitedCount =visitedCount+1
		            	 }
					  	workerList(neighbours(nextWorkerCount)) ! SendMessage
		            }
		        
					import system.dispatcher
		        	val delayTime = scala.concurrent.duration.FiniteDuration(20, "milliseconds")
			   		context.system.scheduler.scheduleOnce(delayTime, self, SendMessage)
		        }
		       if(topology.equalsIgnoreCase("imp2D"))
		       {
		      				
		    	   var numColums:Int = math.ceil(math.sqrt(numberOfNodes)).toInt;
					numNeighbours=0;					
					if(workerNumber-1 >= 0)
					{
						neighbours(numNeighbours) = workerNumber-1;
						numNeighbours=numNeighbours+1
					}
					if(workerNumber-numColums >= 0)
					{
						neighbours(numNeighbours) = workerNumber-numColums;
						numNeighbours=numNeighbours+1
					}				
					if(workerNumber+numColums <= numberOfNodes-1)
					{
						neighbours(numNeighbours) = workerNumber+numColums;
						numNeighbours=numNeighbours+1
					}
					if(workerNumber+1 <= numberOfNodes-1)
					{
						neighbours(numNeighbours) = workerNumber+1;
						numNeighbours=numNeighbours+1
					}
					neighbours(numNeighbours) = numberOfNodes-1-workerNumber;
					numNeighbours=numNeighbours+1
					
					nextWorkerCount =  Random.nextInt(numNeighbours)
					if((nodesAlive(neighbours(nextWorkerCount))==1)&&(!isNodeCompleted))
		            {
		            	 if(!sender.equals(self))
		            	 {
		            		 visitedCount =visitedCount+1
		            	 }
					  	workerList(neighbours(nextWorkerCount)) ! SendMessage
		            }
		        
					import system.dispatcher
		        	val delayTime = scala.concurrent.duration.FiniteDuration(20, "milliseconds")
			   		context.system.scheduler.scheduleOnce(delayTime, self, SendMessage)
		        }
		    }
		    

    /*
     * Push sum implementation logic:
     * 1. Check if the node is converged i.e. the ratio of s/w will be consistent and less than power(10,-10) for 3 consecutive times.
     * 		If yes, change the status of the node as completed and send a message to master as converged.
     * 2. Check if the message sender is same as the receiver. If yes, it should not perform the logic of addition and comparison.
     * 3. If a message is received, it is added with the current value  and the difference if current ratio and previous ratio is measured. If the difference is less than power(10,-10), the node is converged.
     * 4. Message is sent to a node only if it is alive.  
     * 
     */
    
    case PushSumPair(sReceived:Double,wReceived:Double) =>
    
      if(!isNodeCompleted )
      {
       
       if(visitedCount ==3)
       {
        isNodeCompleted =true
        workerNumberList.drop(workerNumber)
        nodesAlive(workerNumber)=0
        master ! PushSumNodeConverged(ratio)
       }
       
       if(!sender.equals(self))
       {
       		sIntermediate= sReceived + sCurrentValue
      		wIntermediate = wReceived + wCurrentValue
      		
  			if(math.abs(ratio-(sIntermediate/wIntermediate)) <= math.pow(10,-10))
    		{
    		  visitedCount = visitedCount + 1
    		}
    		else
    		{
    		  visitedCount  = 0
    		}
       }
       
       		if(topology.equalsIgnoreCase("full"))
       		{
	    			nextWorkerCount =  scala.util.Random.nextInt(numberOfNodes)
	    			neighbour = nextWorkerCount 
       		}
	    	else
	    	{
	    			nextWorkerCount =  scala.util.Random.nextInt(numNeighbours)
	    			neighbour = neighbours(nextWorkerCount)
	    	}
       		
	    	if(sender.equals(self))
	    	  {
	    	   
	    	   if(nodesAlive(nextWorkerCount)==1)
	    	   {
	    	     workerList(neighbour) ! PushSumPair(sCurrentValue,wCurrentValue)
	    	   }
	    	  }
	    	 else
	    	 {
	    	   sCurrentValue  = sIntermediate/2
	    		wCurrentValue  = wIntermediate/2
	    		ratio = sCurrentValue/wCurrentValue
	    		workerList(neighbour) ! PushSumPair(sCurrentValue,wCurrentValue)
	    	 }    
	    	   import system.dispatcher
	    	   val delayTime = scala.concurrent.duration.FiniteDuration(100, "milliseconds")
	    	   context.system.scheduler.scheduleOnce(delayTime, self, PushSumPair(sCurrentValue,wCurrentValue))
	
      } 
 
   }
}

/*
 * All the case classes used for message propagation and push sum computation
 */
sealed trait GossipMessage
case class PushSumPair(s:Double,w:Double) extends GossipMessage;
case object StartComputation extends GossipMessage
case object SendMessage extends GossipMessage
case object NodeVisited extends GossipMessage
case object NodeCompleted extends GossipMessage
case class NodeRemove(node:ActorRef) extends GossipMessage
case class PushSumNodeConverged(sumRatio:Double) extends GossipMessage
case object PushMessage extends GossipMessage

