/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements...
 * ...
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.{Logging, LoggingMarkers, MetricEmitter, TransactionId}
import org.apache.openwhisk.core.connector.MessageFeed
import org.apache.openwhisk.core.entity.ExecManifest.ReactivePrewarmingConfig
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.{Random, Try}
import java.time.Instant

import org.apache.openwhisk.core.entity.ExecutableWhiskAction

case class ColdStartKey(kind: String, memory: ByteSize)

case object EmitMetrics

case object AdjustPrewarmedContainer

/**
 * A pool managing containers to run actions on.
 *
 * This pool fulfills the other half of the ContainerProxy contract. Only
 * one job (either Start or Run) is sent to a child-actor at any given
 * time. The pool then waits for a response of that container, indicating
 * the container is done with the job. Only then will the pool send another
 * request to that container.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 *
 * @param childFactory method to create new container proxy actor
 * @param feed actor to request more work from
 * @param prewarmConfig optional settings for container prewarming
 * @param poolConfig config for the ContainerPool
 */
class ContainerPool(childFactory: ActorRefFactory => ActorRef,
                    feed: ActorRef,
                    prewarmConfig: List[PrewarmingConfig] = List.empty,
                    poolConfig: ContainerPoolConfig)(implicit val logging: Logging)
    extends Actor {
  import ContainerPool.memoryConsumptionOf

  // //////////////////////////////////////////////////
  // Scheduling Policy
  val schedulingPolicy: String = "GCMitigation" // Changed to "GCMitigation"
  var initialRunMessage: Option[Run] = None
  val numContainers: Int = 2 // Number of containers for RoundRobin policy

  // val requestHeapMargin: Long = 5 * 1024 * 1024L // Configurable margin
  val heapGrowthSize: Long = 462424 // can be also 470K
  val nextGCThreshold: Long = 4194304
  var activationTracker: Int = 0
  var EMContainerIndex = 0
  // //////////////////////////////////////////////////

  implicit val ec = context.dispatcher

  var freePool = immutable.Map.empty[ActorRef, ContainerData]
  var busyPool = immutable.Map.empty[ActorRef, ContainerData]
  var prewarmedPool = immutable.Map.empty[ActorRef, PreWarmedData]
  var prewarmStartingPool = immutable.Map.empty[ActorRef, (String, ByteSize)]
  // If all memory slots are occupied and if there is currently no container to be removed, then the actions will be
  // buffered here to keep order of computation.
  // Otherwise actions with small memory-limits could block actions with large memory limits.
  var runBuffer = immutable.Queue.empty[Run]
  // Track the resent buffer head - so that we don't resend buffer head multiple times
  var resent: Option[Run] = None
  val logMessageInterval = 10.seconds
  // Periodically emit metrics (don't need to do this for each message!)
  context.system.scheduler.scheduleAtFixedRate(30.seconds, 10.seconds, self, EmitMetrics)

  // Key is ColdStartKey, value is the number of cold starts in a minute
  var coldStartCount = immutable.Map.empty[ColdStartKey, Int]

  // Define the actionContainers map with the sealed trait
  sealed trait ActionContainerData

  case class SingleContainerData(container: (ActorRef, ContainerData)) extends ActionContainerData

  case class RoundRobinContainerData(containers: List[(ActorRef, ContainerData)], nextIndex: Int)
      extends ActionContainerData

  var actionContainers: Map[FullyQualifiedEntityName, ActionContainerData] = Map.empty

  adjustPrewarmedContainer(true, false)

  // Check periodically, adjust prewarmed container (delete if unused for some time and create some increment containers)
  // Add some random amount to this schedule to avoid a herd of container removal + creation
  val interval = poolConfig.prewarmExpirationCheckInterval + poolConfig.prewarmExpirationCheckIntervalVariance
    .map(v => Random.nextInt(v.toSeconds.toInt))
    .getOrElse(0)
    .seconds
  if (prewarmConfig.exists(!_.reactive.isEmpty)) {
    context.system.scheduler.scheduleAtFixedRate(
      poolConfig.prewarmExpirationCheckInitDelay,
      interval,
      self,
      AdjustPrewarmedContainer)
  }

  def logContainerStart(r: Run, containerState: String, activeActivations: Int, container: Option[Container]): Unit = {
    val namespaceName = r.msg.user.namespace.name.asString
    val actionName = r.action.name.name
    val actionNamespace = r.action.namespace.namespace
    val maxConcurrent = r.action.limits.concurrency.maxConcurrent
    val activationId = r.msg.activationId.toString

    r.msg.transid.mark(
      this,
      LoggingMarkers.INVOKER_CONTAINER_START(containerState, namespaceName, actionNamespace, actionName),
      s"containerStart containerState: $containerState container: $container activations: $activeActivations of max $maxConcurrent action: $actionName namespace: $namespaceName activationId: $activationId",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      val memory = r.action.limits.memory.megabytes.MB
      val actionName = r.action.fullyQualifiedName(false)

      // Capture the initial Run message
      if (initialRunMessage.isEmpty) {
        initialRunMessage = Some(r)
        logging.info(this, s"******************************************************************************")
        logging.info(this, s"Captured initial Run message for action: ${r.action.fullyQualifiedName(false)}")
        logging.info(this, s"******************************************************************************")
        logging.info(this, s"Starting containers for policy: $schedulingPolicy")
        logging.info(this, s"******************************************************************************")
        // start containers based on scheduling policy
        schedulingPolicy match {
          case "SingleContainer" =>
            // Create only one new container
            val container = createContainer(memory)
            val (actorRef, containerData) = container
            val newData = containerData.nextRun(r)
            actionContainers = actionContainers + (actionName -> SingleContainerData((actorRef, newData)))
            // send empty activation --> removes cold start
            actorRef ! r
            // cold activation
            logContainerStart(r, "cold", newData.activeActivationCount, newData.getContainer)

            case "RoundRobin" | "GCMitigation" =>
            // Create multiple containers based on numContainers
            val newContainers = (1 to numContainers).map { _ =>
              createContainer(memory)
            }.toList
            // Send empty request to the first container
            val (actorRef, containerData) = newContainers(0)
            val newData = containerData.nextRun(r)
            val updatedContainersWithNewData = newContainers.updated(0, (actorRef, newData))
            actionContainers = actionContainers + (actionName -> RoundRobinContainerData(
              updatedContainersWithNewData,
              1))
            // send empty activation --> removes cold start
            actorRef ! r
            // Send empty activation to second container
            val (actorRef1, containerData1) = newContainers(1)
            val newData1 = containerData1.nextRun(r)
            val updatedContainersWithNewData1 = newContainers.updated(1, (actorRef1, newData1))
            actionContainers = actionContainers + (actionName -> RoundRobinContainerData(
              updatedContainersWithNewData1,
              0))
            // send empty activation --> removes cold start
            actorRef1 ! r
            logContainerStart(r, "cold", newData1.activeActivationCount, newData1.getContainer)

            // // Add this code to move container from freePool to busyPool
            // freePool = freePool - actorRef
            // busyPool = busyPool + (actorRef -> newData)
        }
      }
      
      schedulingPolicy match {
        case "GCMitigation" =>
          actionContainers.get(actionName) match {
            case Some(RoundRobinContainerData(containers, nextIndex)) =>
              // increment activationTracker until 6, then reset
              activationTracker = activationTracker + 1
              if (activationTracker == 6) {
                // TODO: send fake request to current container
                // val activationRequest = createModifiedRun(r)
                val (actorRef, containerData) = containers(EMContainerIndex)
                val newData = containerData.nextRun(r)
                val updatedContainers = containers.updated(EMContainerIndex, (actorRef, newData))
                actionContainers =
                  actionContainers.updated(actionName, RoundRobinContainerData(updatedContainers, EMContainerIndex))
                actorRef ! r
                logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
                // reset activationTracker
                activationTracker = 0
                // increment EMContainerIndex
                EMContainerIndex = (EMContainerIndex + 1) % containers.size
              }
              // send the activation to the nextIndex container
              val (actorRef, containerData) = containers(EMContainerIndex)
              val newData = containerData.nextRun(r)
              val updatedContainers = containers.updated(EMContainerIndex, (actorRef, newData))
              // val EMContainerIndex = (nextIndex + 1) % containers.size
              actionContainers =
                actionContainers.updated(actionName, RoundRobinContainerData(updatedContainers, EMContainerIndex))
              actorRef ! r
              logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
              resent = None

              //   case None =>
              //     // All containers are close to GC, send fake activation to trigger GC
              //     val activationRequest = createModifiedRun(r)
              //     containers.foreach { case (actorRef, containerData) =>
              //       actorRef ! activationRequest
              //     }
              //     // Buffer the request
              //     runBuffer = runBuffer.enqueue(r)
              //     logging.info(this, s"All containers are undergoing GC; buffering request.")
              // }
            
            case _ =>
              // Handle unexpected data in GCMitigation policy
                logging.error(this, s"Expected RoundRobinContainerData but found unexpected data for action $actionName: ${actionContainers(actionName)}")
              // fail fast
              context.stop(self)
          }

        case "RoundRobin" =>
          actionContainers.get(actionName) match {
            case Some(RoundRobinContainerData(containers, nextIndex)) =>
              // retrieve the nextIndex container
              val (actorRef, containerData) = containers(nextIndex)
              val newData = containerData.nextRun(r)
              val updatedContainers = containers.updated(nextIndex, (actorRef, newData))
              val updatedNextIndex = (nextIndex + 1) % containers.size
              actionContainers =
                actionContainers.updated(actionName, RoundRobinContainerData(updatedContainers, updatedNextIndex))
              actorRef ! r
              logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
              resent = None
            case _ =>
              // Handle unexpected data in GCMitigation policy
                logging.error(this, s"Expected RoundRobinContainerData but found unexpected data for action $actionName: ${actionContainers(actionName)}")
              // fail fast
              context.stop(self)
          }


        case "SingleContainer" =>
          actionContainers.get(actionName) match {
            case Some(SingleContainerData((actorRef, containerData))) =>
              val newData = containerData.nextRun(r)
              actionContainers = actionContainers + (actionName -> SingleContainerData((actorRef, newData)))
              actorRef ! r
              // warm activation
              logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
              resent = None
            case _ =>
              // Handle unexpected data in policy
                logging.error(this, s"Expected SingleContainerData but found unexpected data for action $actionName: ${actionContainers(actionName)}")
              // fail fast
              context.stop(self)
          }

        case _ =>
          logging.error(this, s"Unknown scheduling policy: $schedulingPolicy")
          // fail fast, stop execution
          context.stop(self)
      }

      // Process the next item in the buffer or feed
      processBufferOrFeed()

    // Container is free to take more work
    case NeedWork(containerData) =>
      containerData match {
        case data: WarmedData =>
          handleNeedWork(data)
        case data: WarmingData =>
          handleNeedWork(data)
        case data: WarmingColdData =>
          handleNeedWork(data)
        case _ =>
          logging.warn(this, s"Received NeedWork with unexpected containerData type: ${containerData.getClass}")
      }
      processBufferOrFeed()

    // Container got removed
    case ContainerRemoved(replacePrewarm) =>
      // If container was in free pool, it may have been processing (but under capacity),
      // so there is capacity to accept another job request
      freePool.get(sender()).foreach { _ =>
        freePool = freePool - sender()
      }

      // Container was busy (busy indicates at full capacity), so there is capacity to accept another job request
      busyPool.get(sender()).foreach { _ =>
        busyPool = busyPool - sender()
      }
      processBufferOrFeed()

      // In case this was a prewarm
      prewarmedPool.get(sender()).foreach { _ =>
        prewarmedPool = prewarmedPool - sender()
      }

      // In case this was a starting prewarm
      prewarmStartingPool.get(sender()).foreach { _ =>
        logging.info(this, "failed starting prewarm, removed")
        prewarmStartingPool = prewarmStartingPool - sender()
      }

      // Backfill prewarms on every ContainerRemoved(replacePrewarm = true), just in case
      if (replacePrewarm) {
        adjustPrewarmedContainer(false, false) // In case a prewarm is removed due to health failure or crash
      }

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob =>
      freePool = freePool - sender()
      busyPool = busyPool - sender()
    case EmitMetrics =>
      emitMetrics()

    case AdjustPrewarmedContainer =>
      adjustPrewarmedContainer(false, true)

    case UpdateContainerData(newData) =>
      val actorRef = sender()
      if (freePool.contains(actorRef)) {
        freePool = freePool + (actorRef -> newData)
      } else if (busyPool.contains(actorRef)) {
        busyPool = busyPool + (actorRef -> newData)
      } else {
        // Handle the case where the actorRef is not found
        logging.warn(this, s"Received UpdateContainerData from unknown actor $actorRef")
      }
  }

  // Helper method to handle containerData types with 'action'
  def handleNeedWork(data: ContainerData): Unit = {
    val actionNameOpt = data match {
      case d: WarmedData      => Some(d.action.fullyQualifiedName(false))
      case d: WarmingData     => Some(d.action.fullyQualifiedName(false))
      case d: WarmingColdData => Some(d.action.fullyQualifiedName(false))
      case _ =>
        logging.warn(this, s"Unexpected containerData type in handleNeedWork: ${data.getClass}")
        None
    }

    actionNameOpt.foreach { actionName =>
      actionContainers.get(actionName) match {
        case Some(SingleContainerData((actorRef, existingData))) if actorRef == sender() =>
          val newData = existingData match {
            case warmData: WarmedData =>
              // Update the GC metrics from the received containerData
                warmData.copy(
                lastUsed = Instant.now,
                activeActivationCount = warmData.activeActivationCount - 1,
                gcMetric = data.gcMetric
              )
            case warmingData: WarmingData =>
              warmingData.copy(
              lastUsed = Instant.now,
              activeActivationCount = warmingData.activeActivationCount - 1
            )
            case warmingColdData: WarmingColdData =>
              warmingColdData.copy(
                activeActivationCount = warmingColdData.activeActivationCount - 1
              )
            case _ =>
              logging.warn(this, s"Received NeedWork with unexpected existingData type: ${existingData.getClass}")
              existingData
          }
          actionContainers = actionContainers + (actionName -> SingleContainerData((actorRef, newData)))
          // Move container from busyPool to freePool
          busyPool = busyPool - actorRef
          freePool = freePool + (actorRef -> newData)

        case Some(SingleContainerData((actorRef, existingData))) =>
          // Handle the case when actorRef != sender()
          logging.warn(this, s"Received NeedWork from unexpected actorRef in SingleContainerData")

        case Some(RoundRobinContainerData(containers, nextIndex)) =>
          val index = containers.indexWhere { case (actorRef, _) => actorRef == sender() }
          if (index >= 0) {
            val (actorRef, existingData) = containers(index)
            val newData = existingData match {
              case warmData: WarmedData =>
                // Update the GC metrics from the received containerData
                  warmData.copy(
                  lastUsed = Instant.now,
                  activeActivationCount = warmData.activeActivationCount - 1,
                  gcMetric = data.gcMetric
                )
              case warmingData: WarmingData =>
                warmingData.copy(
                  lastUsed = Instant.now,
                  activeActivationCount = warmingData.activeActivationCount - 1
                )
              case warmingColdData: WarmingColdData =>
                warmingColdData.copy(
                  activeActivationCount = warmingColdData.activeActivationCount - 1
                )
              case _ =>
                logging.warn(this, s"Received NeedWork with unexpected existingData type: ${existingData.getClass}")
                existingData
            }
            val updatedContainers = containers.updated(index, (actorRef, newData))
            actionContainers = actionContainers + (actionName -> RoundRobinContainerData(updatedContainers, nextIndex))
            // Move container from busyPool to freePool
            busyPool = busyPool - actorRef
            freePool = freePool + (actorRef -> newData)
          } else {
            logging.warn(this, s"Received NeedWork with mismatched containerData in actionContainers")
          }

        case Some(otherData) =>
          logging.warn(this, s"Received NeedWork with mismatched containerData in actionContainers")
        case None =>
          logging.warn(this, s"Received NeedWork with mismatched containerData in actionContainers")
      }
    }
  }

  /** Resend next item in the buffer, or trigger next item in the feed, if no items in the buffer. */
  def processBufferOrFeed() = {
    // If buffer has more items, and head has not already been resent, send next one, otherwise get next from feed.
    runBuffer.dequeueOption match {
      case Some((run, q)) => // Run the first from buffer
        implicit val tid = run.msg.transid
        // Avoid sending duplicates
        if (resent.isEmpty) {
          logging.info(this, s"re-processing from buffer (${runBuffer.length} items in buffer)")
          resent = Some(run)
          runBuffer = q // Update the buffer to the remaining items
          self ! run
        } else {
          // Do not resend the buffer head multiple times (may reach this point from multiple messages, before the buffer head is re-processed)
        }
      case None => // Feed me!
        feed ! MessageFeed.Processed
    }
  }

  // Method to create a modified Run message using the stored initial Run message
  def createModifiedRun(originalRun: Run): Run = {
    initialRunMessage.map { storedRun =>
      val newActivationId = ActivationId.generate()
      val modifiedMessage = storedRun.msg.copy(
        activationId = newActivationId,
        transid = TransactionId.invokerNanny
        // You can modify other fields if necessary
      )
      Run(storedRun.action, modifiedMessage)
    }.getOrElse {
      // If we haven't captured an initial Run message, fall back to originalRun
      logging.warn(this, "No initial Run message captured; using original Run message.")
      originalRun
    }
  }

  /** Adjust prewarm containers up to the configured requirements for each kind/memory combination. */
  def adjustPrewarmedContainer(init: Boolean, scheduled: Boolean): Unit = {
    if (scheduled) {
      // On scheduled time, remove expired prewarms
      ContainerPool.removeExpired(poolConfig, prewarmConfig, prewarmedPool).foreach { p =>
        prewarmedPool = prewarmedPool - p
        p ! Remove
      }
      // On scheduled time, emit cold start counter metric with memory + kind
      coldStartCount foreach { coldStart =>
        val coldStartKey = coldStart._1
        MetricEmitter.emitCounterMetric(
          LoggingMarkers.CONTAINER_POOL_PREWARM_COLDSTART(coldStartKey.memory.toString, coldStartKey.kind))
      }
    }
    // Fill in missing prewarms (replaces any deletes)
    ContainerPool
      .increasePrewarms(init, scheduled, coldStartCount, prewarmConfig, prewarmedPool, prewarmStartingPool)
      .foreach { c =>
        val config = c._1
        val currentCount = c._2._1
        val desiredCount = c._2._2
        if (currentCount < desiredCount) {
          (currentCount until desiredCount).foreach { _ =>
            prewarmContainer(config.exec, config.memoryLimit, config.reactive.map(_.ttl))
          }
        }
      }
    if (scheduled) {
      // Lastly, clear coldStartCounts each time scheduled event is processed to reset counts
      coldStartCount = immutable.Map.empty[ColdStartKey, Int]
    }
  }

  /** Creates a new container and updates state accordingly. */
  def createContainer(memoryLimit: ByteSize): (ActorRef, ContainerData) = {
    val ref = childFactory(context)
    val data = MemoryData(memoryLimit)
    freePool = freePool + (ref -> data)
    ref -> data
  }

  /** Creates a new prewarmed container */
  def prewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize, ttl: Option[FiniteDuration]): Unit = {
    if (hasPoolSpaceFor(busyPool ++ freePool ++ prewarmedPool, prewarmStartingPool, memoryLimit)) {
      val newContainer = childFactory(context)
      prewarmStartingPool = prewarmStartingPool + (newContainer -> (exec.kind, memoryLimit))
      newContainer ! Start(exec, memoryLimit, ttl)
    } else {
      logging.warn(
        this,
        s"Cannot create prewarm container due to reaching the invoker memory limit: ${poolConfig.userMemory.toMB}")
    }
  }

  /** This is only for cold start statistics of prewarm configs, e.g., not blackbox or other configs. */
  def incrementColdStartCount(kind: String, memoryLimit: ByteSize): Unit = {
    prewarmConfig
      .filter { config =>
        kind == config.exec.kind && memoryLimit == config.memoryLimit
      }
      .foreach { _ =>
        val coldStartKey = ColdStartKey(kind, memoryLimit)
        coldStartCount.get(coldStartKey) match {
          case Some(value) => coldStartCount = coldStartCount + (coldStartKey -> (value + 1))
          case None        => coldStartCount = coldStartCount + (coldStartKey -> 1)
        }
      }
  }

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind and memory is found.
   *
   * @param action the action that holds the kind and the required memory.
   * @return the container iff found
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ActorRef, ContainerData)] = {
    val kind = action.exec.kind
    val memory = action.limits.memory.megabytes.MB
    val now = Deadline.now
    prewarmedPool.toSeq
      .sortBy(_._2.expires.getOrElse(now))
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`, _, _)) => true
        case _                                             => false
      }
      .map {
        case (ref, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ref -> data)
          prewarmedPool = prewarmedPool - ref
          // Create a new prewarm container
          // NOTE: prewarming ignores the action code in exec, but this is dangerous as the field is accessible to the
          // factory

          // Get the appropriate ttl from prewarm configs
          val ttl =
            prewarmConfig.find(pc => pc.memoryLimit == memory && pc.exec.kind == kind).flatMap(_.reactive.map(_.ttl))
          prewarmContainer(action.exec, memory, ttl)
          (ref, data)
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(toDelete: ActorRef) = {
    toDelete ! Remove
    freePool = freePool - toDelete
    busyPool = busyPool - toDelete
  }

  /**
   * Calculate if there is enough free memory within a given pool.
   *
   * @param pool The pool, that has to be checked, if there is enough free memory.
   * @param memory The amount of memory to check.
   * @return true, if there is enough space for the given amount of memory.
   */
  def hasPoolSpaceFor[A](pool: Map[A, ContainerData],
                         prewarmStartingPool: Map[A, (String, ByteSize)],
                         memory: ByteSize): Boolean = {
    memoryConsumptionOf(pool) + prewarmStartingPool.map(_._2._2.toMB).sum + memory.toMB <= poolConfig.userMemory.toMB
  }

  /**
   * Log metrics about pool state (buffer size, buffer memory requirements, active number, active memory, prewarm number, prewarm memory)
   */
  private def emitMetrics() = {
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_RUNBUFFER_COUNT, runBuffer.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_RUNBUFFER_SIZE,
      runBuffer.map(_.action.limits.memory.megabytes).sum)
    val containersInUse = freePool.filter(_._2.activeActivationCount > 0) ++ busyPool
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_ACTIVE_COUNT, containersInUse.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_ACTIVE_SIZE,
      containersInUse.map(_._2.memoryLimit.toMB).sum)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_COUNT,
      prewarmedPool.size + prewarmStartingPool.size)
    MetricEmitter.emitGaugeMetric(
      LoggingMarkers.CONTAINER_POOL_PREWARM_SIZE,
      prewarmedPool.map(_._2.memoryLimit.toMB).sum + prewarmStartingPool.map(_._2._2.toMB).sum)
    val unused = freePool.filter(_._2.activeActivationCount == 0)
    val unusedMB = unused.map(_._2.memoryLimit.toMB).sum
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_COUNT, unused.size)
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_IDLES_SIZE, unusedMB)
  }
}

object ContainerPool {

  /**
   * Calculate the memory of a given pool.
   *
   * @param pool The pool with the containers.
   * @return The memory consumption of all containers in the pool in Megabytes.
   */
  protected[containerpool] def memoryConsumptionOf[A](pool: Map[A, ContainerData]): Long = {
    pool.map(_._2.memoryLimit.toMB).sum
  }

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   *
   * @param action the action to run
   * @param invocationNamespace the namespace, that wants to run the action
   * @param idles a map of idle containers, awaiting work
   * @return a container if one found
   */
  protected[containerpool] def schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
    idles
      .find {
        case (_, c @ WarmedData(_, `invocationNamespace`, `action`, _, _, _, _)) if c.hasCapacity() => true
        case _                                                                                   => false
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingData(_, `invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                 => false
        }
      }
      .orElse {
        idles.find {
          case (_, c @ WarmingColdData(`invocationNamespace`, `action`, _, _)) if c.hasCapacity() => true
          case _                                                                                  => false
        }
      }
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   * Depending on the space that has to be allocated, several containers might be removed.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   *
   * @param pool a map of all free containers in the pool
   * @param memory the amount of memory that has to be freed up
   * @return a list of containers to be removed iff found
   */
  @tailrec
  protected[containerpool] def remove[A](pool: Map[A, ContainerData],
                                         memory: ByteSize,
                                         toRemove: List[A] = List.empty): List[A] = {
    // Try to find a Free container that does NOT have any active activations AND is initialized with any OTHER action
    val freeContainers = pool.collect {
      // Only warm containers will be removed. Prewarmed containers will stay always.
      case (ref, w: WarmedData) if w.activeActivationCount == 0 =>
        ref -> w
    }

    if (memory > 0.B && freeContainers.nonEmpty && memoryConsumptionOf(freeContainers) >= memory.toMB) {
      // Remove the oldest container if:
      // - there is more memory required
      // - there are still containers that can be removed
      // - there are enough free containers that can be removed
      val (ref, data) = freeContainers.minBy(_._2.lastUsed)
      // Catch exception if remaining memory will be negative
      val remainingMemory = Try(memory - data.memoryLimit).getOrElse(0.B)
      remove(freeContainers - ref, remainingMemory, toRemove ++ List(ref))
    } else {
      // If this is the first call: All containers are in use currently, or there is more memory needed than
      // containers can be removed.
      // Or, if this is one of the recursions: Enough containers are found to get the memory, that is
      // necessary. -> Abort recursion
      toRemove
    }
  }

  /**
   * Find the expired actor in prewarmedPool
   *
   * @param poolConfig
   * @param prewarmConfig
   * @param prewarmedPool
   * @param logging
   * @return a list of expired actors
   */
  def removeExpired[A](poolConfig: ContainerPoolConfig,
                       prewarmConfig: List[PrewarmingConfig],
                       prewarmedPool: Map[A, PreWarmedData])(implicit logging: Logging): List[A] = {
    val now = Deadline.now
    val expireds = prewarmConfig
      .flatMap { config =>
        val kind = config.exec.kind
        val memory = config.memoryLimit
        config.reactive
          .map { _ =>
            val expiredPrewarmedContainers = prewarmedPool.toSeq
              .filter {
                case (_, p @ PreWarmedData(_, `kind`, `memory`, _, _)) if p.isExpired() => true
                case _                                                                  => false
              }
              .sortBy(_._2.expires.getOrElse(now))

            if (expiredPrewarmedContainers.nonEmpty) {
              // Emit expired container counter metric with memory + kind
              MetricEmitter.emitCounterMetric(LoggingMarkers.CONTAINER_POOL_PREWARM_EXPIRED(memory.toString, kind))
              logging.info(
                this,
                s"[kind: ${kind} memory: ${memory.toString}] ${expiredPrewarmedContainers.size} expired prewarmed containers")
            }
            expiredPrewarmedContainers.map(e => (e._1, e._2.expires.getOrElse(now)))
          }
          .getOrElse(List.empty)
      }
      .sortBy(_._2) // Need to sort these so that if the results are limited, we take the oldest
      .map(_._1)
    if (expireds.nonEmpty) {
      logging.info(this, s"removing up to ${poolConfig.prewarmExpirationLimit} of ${expireds.size} expired containers")
      expireds.take(poolConfig.prewarmExpirationLimit).foreach { e =>
        prewarmedPool.get(e).foreach { d =>
          logging.info(this, s"removing expired prewarm of kind ${d.kind} with container ${d.container}")
        }
      }
    }
    expireds.take(poolConfig.prewarmExpirationLimit)
  }

  /**
   * Find the increased number for the prewarmed kind
   *
   * @param init
   * @param scheduled
   * @param coldStartCount
   * @param prewarmConfig
   * @param prewarmedPool
   * @param prewarmStartingPool
   * @param logging
   * @return the current number and increased number for the kind in the Map
   */
  def increasePrewarms(init: Boolean,
                       scheduled: Boolean,
                       coldStartCount: Map[ColdStartKey, Int],
                       prewarmConfig: List[PrewarmingConfig],
                       prewarmedPool: Map[ActorRef, PreWarmedData],
                       prewarmStartingPool: Map[ActorRef, (String, ByteSize)])(
    implicit logging: Logging): Map[PrewarmingConfig, (Int, Int)] = {
    prewarmConfig.map { config =>
      val kind = config.exec.kind
      val memory = config.memoryLimit

      val runningCount = prewarmedPool.count {
        // Done starting (include expired, since they may not have been removed yet)
        case (_, p @ PreWarmedData(_, `kind`, `memory`, _, _)) => true
        // Started but not finished starting (or expired)
        case _ => false
      }
      val startingCount = prewarmStartingPool.count(p => p._2._1 == kind && p._2._2 == memory)
      val currentCount = runningCount + startingCount

      // Determine how many are needed
      val desiredCount: Int =
        if (init) config.initialCount
        else {
          if (scheduled) {
            // Scheduled/reactive config backfill
            config.reactive
              .map(c => getReactiveCold(coldStartCount, c, kind, memory).getOrElse(c.minCount)) // Reactive -> desired is either cold start driven, or minCount
              .getOrElse(config.initialCount) // Not reactive -> desired is always initial count
          } else {
            // Normal backfill after removal - make sure at least minCount or initialCount is started
            config.reactive.map(_.minCount).getOrElse(config.initialCount)
          }
        }

      if (currentCount < desiredCount) {
        logging.info(
          this,
          s"found ${currentCount} started and ${startingCount} starting; ${if (init) "initing" else "backfilling"} ${desiredCount - currentCount} pre-warms to desired count: ${desiredCount} for kind:${config.exec.kind} mem:${config.memoryLimit.toString}")(
          TransactionId.invokerWarmup)
      }
      (config, (currentCount, desiredCount))
    }.toMap
  }

  /**
   * Get the required prewarmed container number according to the cold starts happened in previous minute
   *
   * @param coldStartCount
   * @param config
   * @param kind
   * @param memory
   * @return the required prewarmed container number
   */
  def getReactiveCold(coldStartCount: Map[ColdStartKey, Int],
                      config: ReactivePrewarmingConfig,
                      kind: String,
                      memory: ByteSize): Option[Int] = {
    coldStartCount.get(ColdStartKey(kind, memory)).map { value =>
      // Let's assume that threshold is `2`, increment is `1` in runtimes.json
      // if cold start number in previous minute is `2`, requireCount is `2/2 * 1 = 1`
      // if cold start number in previous minute is `4`, requireCount is `4/2 * 1 = 2`
      math.min(math.max(config.minCount, (value / config.threshold) * config.increment), config.maxCount)
    }
  }

  def props(factory: ActorRefFactory => ActorRef,
            poolConfig: ContainerPoolConfig,
            feed: ActorRef,
            prewarmConfig: List[PrewarmingConfig] = List.empty)(implicit logging: Logging) =
    Props(new ContainerPool(factory, feed, prewarmConfig, poolConfig))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(initialCount: Int,
                            exec: CodeExec[_],
                            memoryLimit: ByteSize,
                            reactive: Option[ReactivePrewarmingConfig] = None)


/** 
 * Add ContainerProxy class for GPT reference
 * 
 * /*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import java.time.Instant

import akka.actor.Status.{Failure => FailureMessage}
import akka.actor.{FSM, Props, Stash}
import akka.event.Logging.InfoLevel
import akka.io.IO
import akka.io.Tcp
import akka.io.Tcp.Close
import akka.io.Tcp.CommandFailed
import akka.io.Tcp.Connect
import akka.io.Tcp.Connected
import akka.pattern.pipe
import pureconfig.loadConfigOrThrow
import pureconfig.generic.auto._
import java.net.InetSocketAddress
import java.net.SocketException

import org.apache.openwhisk.common.MetricEmitter
import org.apache.openwhisk.common.TransactionId.systemPrefix

import scala.collection.immutable
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.apache.openwhisk.common.{AkkaLogging, Counter, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.ConfigKeys
import org.apache.openwhisk.core.ack.ActiveAck
import org.apache.openwhisk.core.connector.{
  ActivationMessage,
  CombinedCompletionAndResultMessage,
  CompletionMessage,
  ResultMessage
}
import org.apache.openwhisk.core.containerpool.logging.LogCollectingException
import org.apache.openwhisk.core.database.UserContext
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.invoker.Invoker.LogsCollector
import org.apache.openwhisk.http.Messages

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

// States
sealed trait ContainerState
case object Uninitialized extends ContainerState
case object Starting extends ContainerState
case object Started extends ContainerState
case object Running extends ContainerState
case object Ready extends ContainerState
case object Pausing extends ContainerState
case object Paused extends ContainerState
case object Removing extends ContainerState

// Added for GC metrics
case class UpdateContainerData(newData: ContainerData)
// Define the GCMetric case class
case class GCMetric(heapAlloc: Long, nextGC: Long, numGC: Long)

// Updated ContainerData and its subclasses to include GC metrics
sealed abstract class ContainerData(val lastUsed: Instant,
                                    val memoryLimit: ByteSize,
                                    val activeActivationCount: Int,
                                    val gcMetric: Option[GCMetric] = None) {
  def nextRun(r: Run): ContainerData

  /**
   *  Return Some(container) (for ContainerStarted instances) or None(for ContainerNotStarted instances)
   *  Useful for cases where all ContainerData instances are handled, vs cases where only ContainerStarted
   *  instances are handled */
  def getContainer: Option[Container]

  /** String to indicate the state of this container after scheduling */
  val initingState: String

  /** Inidicates whether this container can service additional activations */
  def hasCapacity(): Boolean
}

/** abstract type to indicate an unstarted container */
sealed abstract class ContainerNotStarted(override val lastUsed: Instant,
                                          override val memoryLimit: ByteSize,
                                          override val activeActivationCount: Int)
    extends ContainerData(lastUsed, memoryLimit, activeActivationCount) {
  override def getContainer = None
  override val initingState = "cold"
}

/** abstract type to indicate a started container */
sealed abstract class ContainerStarted(val container: Container,
                                       override val lastUsed: Instant,
                                       override val memoryLimit: ByteSize,
                                       override val activeActivationCount: Int,
                                       override val gcMetric: Option[GCMetric] = None)
    extends ContainerData(lastUsed, memoryLimit, activeActivationCount, gcMetric) {
  override def getContainer = Some(container)
}

/** trait representing a container that is in use and (potentially) usable by subsequent or concurrent activations */
sealed abstract trait ContainerInUse {
  val activeActivationCount: Int
  val action: ExecutableWhiskAction
  def hasCapacity() =
    activeActivationCount < action.limits.concurrency.maxConcurrent
}

/** trait representing a container that is NOT in use and is usable by subsequent activation(s) */
sealed abstract trait ContainerNotInUse {
  def hasCapacity() = true
}

/** type representing a cold (not running) container */
case class NoData(override val activeActivationCount: Int = 0)
    extends ContainerNotStarted(Instant.EPOCH, 0.B, activeActivationCount)
    with ContainerNotInUse {
  override def nextRun(r: Run) = WarmingColdData(r.msg.user.namespace.name, r.action, Instant.now, 1)
}

/** type representing a cold (not running) container with specific memory allocation */
case class MemoryData(override val memoryLimit: ByteSize, override val activeActivationCount: Int = 0)
    extends ContainerNotStarted(Instant.EPOCH, memoryLimit, activeActivationCount)
    with ContainerNotInUse {
  override def nextRun(r: Run) = WarmingColdData(r.msg.user.namespace.name, r.action, Instant.now, 1)
}

/** type representing a prewarmed (running, but unused) container (with a specific memory allocation) */
case class PreWarmedData(override val container: Container,
                         kind: String,
                         override val memoryLimit: ByteSize,
                         override val activeActivationCount: Int = 0,
                         expires: Option[Deadline] = None)
    extends ContainerStarted(container, Instant.EPOCH, memoryLimit, activeActivationCount)
    with ContainerNotInUse {
  override val initingState = "prewarmed"
  override def nextRun(r: Run) =
    WarmingData(container, r.msg.user.namespace.name, r.action, Instant.now, 1)
  def isExpired(): Boolean = expires.exists(_.isOverdue())
}

/** type representing a prewarm (running, but not used) container that is being initialized (for a specific action + invocation namespace) */
case class WarmingData(override val container: Container,
                       invocationNamespace: EntityName,
                       action: ExecutableWhiskAction,
                       override val lastUsed: Instant,
                       override val activeActivationCount: Int = 0)
    extends ContainerStarted(container, lastUsed, action.limits.memory.megabytes.MB, activeActivationCount)
    with ContainerInUse {
  override val initingState = "warming"
  override def nextRun(r: Run) = copy(lastUsed = Instant.now, activeActivationCount = activeActivationCount + 1)
}

/** type representing a cold (not yet running) container that is being initialized (for a specific action + invocation namespace) */
case class WarmingColdData(invocationNamespace: EntityName,
                           action: ExecutableWhiskAction,
                           override val lastUsed: Instant,
                           override val activeActivationCount: Int = 0)
    extends ContainerNotStarted(lastUsed, action.limits.memory.megabytes.MB, activeActivationCount)
    with ContainerInUse {
  override val initingState = "warmingCold"
  override def nextRun(r: Run) = copy(lastUsed = Instant.now, activeActivationCount = activeActivationCount + 1)
}

/** type representing a warm container that has already been in use (for a specific action + invocation namespace) */
case class WarmedData(override val container: Container,
                      invocationNamespace: EntityName,
                      action: ExecutableWhiskAction,
                      override val lastUsed: Instant,
                      override val activeActivationCount: Int = 0,
                      resumeRun: Option[Run] = None,
                      override val gcMetric: Option[GCMetric] = None)
    extends ContainerStarted(container, lastUsed, action.limits.memory.megabytes.MB, activeActivationCount, gcMetric)
    with ContainerInUse {
  override val initingState = "warmed"
  override def nextRun(r: Run) = copy(lastUsed = Instant.now, activeActivationCount = activeActivationCount + 1)
  //track the resuming run for easily referring to the action being resumed (it may fail and be resent)
  def withoutResumeRun() = this.copy(resumeRun = None)
  def withResumeRun(job: Run) = this.copy(resumeRun = Some(job))
}

// Events received by the actor
case class Start(exec: CodeExec[_], memoryLimit: ByteSize, ttl: Option[FiniteDuration] = None)
case class Run(action: ExecutableWhiskAction, msg: ActivationMessage, retryLogDeadline: Option[Deadline] = None)
case object Remove
case class HealthPingEnabled(enabled: Boolean)

// Events sent by the actor
case class NeedWork(data: ContainerData)
case object ContainerPaused
case class ContainerRemoved(replacePrewarm: Boolean) // when container is destroyed
case object RescheduleJob // job is sent back to parent and could not be processed because container is being destroyed
case class PreWarmCompleted(data: PreWarmedData)
case class InitCompleted(data: WarmedData)
case object RunCompleted

/**
 * A proxy that wraps a Container. It is used to keep track of the lifecycle
 * of a container and to guarantee a contract between the client of the container
 * and the container itself.
 *
 * The contract is as follows:
 * 1. If action.limits.concurrency.maxConcurrent == 1:
 *    Only one job is to be sent to the ContainerProxy at one time. ContainerProxy
 *    will delay all further jobs until a previous job has finished.
 *
 *    1a. The next job can be sent to the ContainerProxy after it indicates available
 *       capacity by sending NeedWork to its parent.
 *
 * 2. If action.limits.concurrency.maxConcurrent > 1:
 *    Parent must coordinate with ContainerProxy to attempt to send only data.action.limits.concurrency.maxConcurrent
 *    jobs for concurrent processing.
 *
 *    Since the current job count is only periodically sent to parent, the number of jobs
 *    sent to ContainerProxy may exceed data.action.limits.concurrency.maxConcurrent,
 *    in which case jobs are buffered, so that only a max of action.limits.concurrency.maxConcurrent
 *    are ever sent into the container concurrently. Parent will NOT be signalled to send more jobs until
 *    buffered jobs are completed, but their order is not guaranteed.
 *
 *    2a. The next job can be sent to the ContainerProxy after ContainerProxy has "concurrent capacity",
 *        indicated by sending NeedWork to its parent.
 *
 * 3. A Remove message can be sent at any point in time. Like multiple jobs though,
 *    it will be delayed until the currently running job finishes.
 *
 * @constructor
 * @param factory a function generating a Container
 * @param sendActiveAck a function sending the activation via active ack
 * @param storeActivation a function storing the activation in a persistent store
 * @param unusedTimeout time after which the container is automatically thrown away
 * @param pauseGrace time to wait for new work before pausing the container
 */
class ContainerProxy(factory: (TransactionId,
                               String,
                               ImageName,
                               Boolean,
                               ByteSize,
                               Int,
                               Option[Double],
                               Option[ExecutableWhiskAction]) => Future[Container],
                     sendActiveAck: ActiveAck,
                     storeActivation: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
                     collectLogs: LogsCollector,
                     instance: InvokerInstanceId,
                     poolConfig: ContainerPoolConfig,
                     healtCheckConfig: ContainerProxyHealthCheckConfig,
                     activationErrorLoggingConfig: ContainerProxyActivationErrorLogConfig,
                     unusedTimeout: FiniteDuration,
                     pauseGrace: FiniteDuration,
                     testTcp: Option[ActorRef])
    extends FSM[ContainerState, ContainerData]
    with Stash {
  implicit val ec = context.system.dispatcher
  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ac = context.system
  var rescheduleJob = false // true iff actor receives a job but cannot process it because actor will destroy itself
  var runBuffer = immutable.Queue.empty[Run] //does not retain order, but does manage jobs that would have pushed past action concurrency limit
  //track buffer processing state to avoid extra transitions near end of buffer - this provides a pseudo-state between Running and Ready
  var bufferProcessing = false

  //keep a separate count to avoid confusion with ContainerState.activeActivationCount that is tracked/modified only in ContainerPool
  var activeCount = 0;
  var healthPingActor: Option[ActorRef] = None //setup after prewarm starts
  val tcp: ActorRef = testTcp.getOrElse(IO(Tcp)) //allows to testing interaction with Tcp extension

  startWith(Uninitialized, NoData())

  when(Uninitialized) {
    // pre warm a container (creates a stem cell container)
    case Event(job: Start, _) =>
      factory(
        TransactionId.invokerWarmup,
        ContainerProxy.containerName(instance, "prewarm", job.exec.kind),
        job.exec.image,
        job.exec.pull,
        job.memoryLimit,
        poolConfig.cpuShare(job.memoryLimit),
        poolConfig.cpuLimit(job.memoryLimit),
        None)
        .map(container =>
          PreWarmCompleted(PreWarmedData(container, job.exec.kind, job.memoryLimit, expires = job.ttl.map(_.fromNow))))
        .pipeTo(self)

      goto(Starting)

    // cold start (no container to reuse or available stem cell container)
    case Event(job: Run, _) =>
      implicit val transid = job.msg.transid
      activeCount += 1
      // create a new container
      val container = factory(
        job.msg.transid,
        ContainerProxy.containerName(instance, job.msg.user.namespace.name.asString, job.action.name.asString),
        job.action.exec.image,
        job.action.exec.pull,
        job.action.limits.memory.megabytes.MB,
        poolConfig.cpuShare(job.action.limits.memory.megabytes.MB),
        poolConfig.cpuLimit(job.action.limits.memory.megabytes.MB),
        Some(job.action))

      // container factory will either yield a new container ready to execute the action, or
      // starting up the container failed; for the latter, it's either an internal error starting
      // a container or a docker action that is not conforming to the required action API
      container
        .andThen {
          case Success(container) =>
            // the container is ready to accept an activation; register it as PreWarmed; this
            // normalizes the life cycle for containers and their cleanup when activations fail
            self ! PreWarmCompleted(
              PreWarmedData(container, job.action.exec.kind, job.action.limits.memory.megabytes.MB, 1, expires = None))

          case Failure(t) =>
            // the container did not come up cleanly, so disambiguate the failure mode and then cleanup
            // the failure is either the system fault, or for docker actions, the application/developer fault
            val response = t match {
              case WhiskContainerStartupError(msg) => ActivationResponse.whiskError(msg)
              case BlackboxStartupError(msg)       => ActivationResponse.developerError(msg)
              case _                               => ActivationResponse.whiskError(Messages.resourceProvisionError)
            }
            val context = UserContext(job.msg.user)
            // construct an appropriate activation and record it in the datastore,
            // also update the feed and active ack; the container cleanup is queued
            // implicitly via a FailureMessage which will be processed later when the state
            // transitions to Running
            val activation = ContainerProxy.constructWhiskActivation(job, None, Interval.zero, false, response)
            sendActiveAck(
              transid,
              activation,
              job.msg.blocking,
              job.msg.rootControllerIndex,
              job.msg.user.namespace.uuid,
              CombinedCompletionAndResultMessage(transid, activation, instance))
            storeActivation(transid, activation, job.msg.blocking, context)
        }
        .flatMap { container =>
          // now attempt to inject the user code and run the action
          initializeAndRun(container, job)
            .map(_ => RunCompleted)
        }
        .pipeTo(self)

      goto(Running)
  }

  when(Starting) {
    // container was successfully obtained
    case Event(completed: PreWarmCompleted, _) =>
      context.parent ! NeedWork(completed.data)
      goto(Started) using completed.data

    // container creation failed
    case Event(_: FailureMessage, _) =>
      context.parent ! ContainerRemoved(true)
      stop()

    case _ => delay
  }

  when(Started) {
    case Event(job: Run, data: PreWarmedData) =>
      implicit val transid = job.msg.transid
      activeCount += 1
      initializeAndRun(data.container, job)
        .map(_ => RunCompleted)
        .pipeTo(self)
      goto(Running) using PreWarmedData(data.container, data.kind, data.memoryLimit, 1, data.expires)

    case Event(Remove, data: PreWarmedData) => destroyContainer(data, false)

    // prewarm container failed
    case Event(_: FailureMessage, data: PreWarmedData) =>
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_PREWARM)
      destroyContainer(data, true)
  }

  when(Running) {
    // Intermediate state, we were able to start a container
    // and we keep it in case we need to destroy it.
    case Event(completed: PreWarmCompleted, _) => stay using completed.data

    // Run during prewarm init (for concurrent > 1)
    case Event(job: Run, data: PreWarmedData) =>
      implicit val transid = job.msg.transid
      logging.info(this, s"buffering for warming container ${data.container}; ${activeCount} activations in flight")
      runBuffer = runBuffer.enqueue(job)
      stay()

    // Run during cold init (for concurrent > 1)
    case Event(job: Run, _: NoData) =>
      implicit val transid = job.msg.transid
      logging.info(this, s"buffering for cold warming container ${activeCount} activations in flight")
      runBuffer = runBuffer.enqueue(job)
      stay()

    // Init was successful
    case Event(completed: InitCompleted, _: PreWarmedData) =>
      processBuffer(completed.data.action, completed.data)
      stay using completed.data

    // Init was successful
    case Event(data: WarmedData, _: PreWarmedData) =>
      //in case concurrency supported, multiple runs can begin as soon as init is complete
      context.parent ! NeedWork(data)
      stay using data

    // Run was successful
    case Event(RunCompleted, data: WarmedData) =>
      activeCount -= 1
      val newData = data.withoutResumeRun()
      //if there are items in runbuffer, process them if there is capacity, and stay; otherwise if we have any pending activations, also stay
      if (requestWork(data) || activeCount > 0) {
        stay using newData
      } else {
        goto(Ready) using newData
      }
    case Event(job: Run, data: WarmedData)
        if activeCount >= data.action.limits.concurrency.maxConcurrent && !rescheduleJob => //if we are over concurrency limit, and not a failure on resume
      implicit val transid = job.msg.transid
      logging.warn(this, s"buffering for maxed warm container ${data.container}; ${activeCount} activations in flight")
      runBuffer = runBuffer.enqueue(job)
      stay()
    case Event(job: Run, data: WarmedData)
        if activeCount < data.action.limits.concurrency.maxConcurrent && !rescheduleJob => //if there was a delay, and not a failure on resume, skip the run
      activeCount += 1
      implicit val transid = job.msg.transid
      bufferProcessing = false //reset buffer processing state
      initializeAndRun(data.container, job)
        .map(_ => RunCompleted)
        .pipeTo(self)
      stay() using data

    //ContainerHealthError should cause rescheduling of the job
    case Event(FailureMessage(e: ContainerHealthError), data: WarmedData) =>
      implicit val tid = e.tid
      MetricEmitter.emitCounterMetric(LoggingMarkers.INVOKER_CONTAINER_HEALTH_FAILED_WARM)
      //resend to self will send to parent once we get to Removing state
      val newData = data.resumeRun
        .map { run =>
          logging.warn(this, "Ready warm container unhealthy, will retry activation.")
          self ! run
          data.withoutResumeRun()
        }
        .getOrElse(data)
      rescheduleJob = true
      rejectBuffered()
      destroyContainer(newData, true)

    // Failed after /init (the first run failed) on prewarmed or cold start
    // - container will be destroyed
    // - buffered will be aborted (if init fails, we assume it will always fail)
    case Event(f: FailureMessage, data: PreWarmedData) =>
      logging.error(
        this,
        s"Failed during init of cold container ${data.getContainer}, queued activations will be aborted.")

      activeCount -= 1
      //reuse an existing init failure for any buffered activations that will be aborted
      val r = f.cause match {
        case ActivationUnsuccessfulError(r) => Some(r.response)
        case _                              => None
      }
      destroyContainer(data, true, true, r)

    // Failed for a subsequent /run
    // - container will be destroyed
    // - buffered will be resent (at least 1 has completed, so others are given a chance to complete)
    case Event(_: FailureMessage, data: WarmedData) =>
      logging.error(
        this,
        s"Failed during use of warm container ${data.getContainer}, queued activations will be resent.")
      activeCount -= 1
      if (activeCount == 0) {
        destroyContainer(data, true)
      } else {
        //signal that this container is going away (but don't remove it yet...)
        rescheduleJob = true
        goto(Removing)
      }

    // Failed at getting a container for a cold-start run
    // - container will be destroyed
    // - buffered will be aborted (if cold start container fails to start, we assume it will continue to fail)
    case Event(_: FailureMessage, _) =>
      logging.error(this, "Failed to start cold container, queued activations will be aborted.")
      activeCount -= 1
      context.parent ! ContainerRemoved(true)
      abortBuffered()
      rescheduleJob = true
      goto(Removing)

    case _ => delay
  }

when(Ready) {
  case Event(StateTimeout, data: WarmedData) =>
    // Do not transition to Pausing state due to idle timeout
    stay

  case Event(Remove, _) =>
    // Ignore remove messages
    stay

  case Event(r: Run, data: WarmedData) =>
    implicit val transid = r.msg.transid
    activeCount += 1
    initializeAndRun(data.container, r)
      .map(_ => RunCompleted)
      .pipeTo(self)
    goto(Running) using data
}


  when(Pausing) {
    case Event(ContainerPaused, data: WarmedData)   => goto(Paused)
    case Event(_: FailureMessage, data: WarmedData) => destroyContainer(data, true)
    case _                                          => delay
  }

  when(Paused, stateTimeout = unusedTimeout) {
    case Event(job: Run, data: WarmedData) =>
      implicit val transid = job.msg.transid
      activeCount += 1
      val newData = data.withResumeRun(job)
      data.container
        .resume()
        .andThen {
          // Sending the message to self on a failure will cause the message
          // to ultimately be sent back to the parent (which will retry it)
          // when container removal is done.
          case Failure(_) =>
            rescheduleJob = true
            self ! job
        }
        .flatMap(_ => initializeAndRun(data.container, job, true))
        .map(_ => RunCompleted)
        .pipeTo(self)
      goto(Running) using newData

    // container is reclaimed by the pool or it has become too old
    case Event(StateTimeout | Remove, data: WarmedData) =>
      rescheduleJob = true // to suppress sending message to the pool and not double count
      destroyContainer(data, true)
  }

  when(Removing) {
    case Event(job: Run, _) =>
      // Send the job back to the pool to be rescheduled
      context.parent ! job
      stay
    // Run was successful, after another failed concurrent Run
    case Event(RunCompleted, data: WarmedData) =>
      activeCount -= 1
      val newData = data.withoutResumeRun()
      //if there are items in runbuffer, process them if there is capacity, and stay; otherwise if we have any pending activations, also stay
      if (activeCount == 0) {
        destroyContainer(newData, true)
      } else {
        stay using newData
      }
    case Event(ContainerRemoved(_), _) =>
      stop()
    // Run failed, after another failed concurrent Run
    case Event(_: FailureMessage, data: WarmedData) =>
      activeCount -= 1
      val newData = data.withoutResumeRun()
      if (activeCount == 0) {
        destroyContainer(newData, true)
      } else {
        stay using newData
      }
  }

  // Unstash all messages stashed while in intermediate state
  onTransition {
    case _ -> Started =>
      if (healtCheckConfig.enabled) {
        logging.debug(this, "enabling health ping on Started")
        nextStateData.getContainer.foreach { c =>
          enableHealthPing(c)
        }
      }
      unstashAll()
    case _ -> Running =>
      if (healtCheckConfig.enabled && healthPingActor.isDefined) {
        logging.debug(this, "disabling health ping on Running")
        disableHealthPing()
      }
    case _ -> Ready =>
      unstashAll()
    case _ -> Paused =>
      unstashAll()
    case _ -> Removing =>
      unstashAll()
  }

  initialize()

  /** Either process runbuffer or signal parent to send work; return true if runbuffer is being processed */
  def requestWork(newData: WarmedData): Boolean = {
    //if there is concurrency capacity, process runbuffer, signal NeedWork, or both
    if (activeCount < newData.action.limits.concurrency.maxConcurrent) {
      if (runBuffer.nonEmpty) {
        //only request work once, if available larger than runbuffer
        val available = newData.action.limits.concurrency.maxConcurrent - activeCount
        val needWork: Boolean = available > runBuffer.size
        processBuffer(newData.action, newData)
        if (needWork) {
          //after buffer processing, then send NeedWork
          context.parent ! NeedWork(newData)
        }
        true
      } else {
        context.parent ! NeedWork(newData)
        bufferProcessing //true in case buffer is still in process
      }
    } else {
      false
    }
  }

  /** Process buffered items up to the capacity of action concurrency config */
  def processBuffer(action: ExecutableWhiskAction, newData: ContainerData) = {
    //send as many buffered as possible
    val available = action.limits.concurrency.maxConcurrent - activeCount
    logging.info(this, s"resending up to ${available} from ${runBuffer.length} buffered jobs")
    1 to available foreach { _ =>
      runBuffer.dequeueOption match {
        case Some((run, q)) =>
          self ! run
          bufferProcessing = true
          runBuffer = q
        case _ =>
      }
    }
  }

  /** Delays all incoming messages until unstashAll() is called */
  def delay = {
    stash()
    stay
  }

  /**
   * Destroys the container after unpausing it if needed. Can be used
   * as a state progression as it goes to Removing.
   *
   * @param newData the ContainerStarted which container will be destroyed
   */
  def destroyContainer(newData: ContainerStarted,
                       replacePrewarm: Boolean,
                       abort: Boolean = false,
                       abortResponse: Option[ActivationResponse] = None) = {
    val container = newData.container
    if (!rescheduleJob) {
      context.parent ! ContainerRemoved(replacePrewarm)
    } else {
      context.parent ! RescheduleJob
    }
    val abortProcess = if (abort && runBuffer.nonEmpty) {
      abortBuffered(abortResponse)
    } else {
      rejectBuffered()
      Future.successful(())
    }

    val unpause = stateName match {
      case Paused => container.resume()(TransactionId.invokerNanny)
      case _      => Future.successful(())
    }

    unpause
      .flatMap(_ => container.destroy()(TransactionId.invokerNanny))
      .flatMap(_ => abortProcess)
      .map(_ => ContainerRemoved(replacePrewarm))
      .pipeTo(self)
    if (stateName != Removing) {
      goto(Removing) using newData
    } else {
      stay using newData
    }
  }

  def abortBuffered(abortResponse: Option[ActivationResponse] = None): Future[Any] = {
    logging.info(this, s"aborting ${runBuffer.length} queued activations after failed init or failed cold start")
    val f = runBuffer.flatMap { job =>
      implicit val tid = job.msg.transid
      logging.info(
        this,
        s"aborting activation ${job.msg.activationId} after failed init or cold start with ${abortResponse}")
      val result = ContainerProxy.constructWhiskActivation(
        job,
        None,
        Interval.zero,
        false,
        abortResponse.getOrElse(ActivationResponse.whiskError(Messages.abnormalRun)))
      val context = UserContext(job.msg.user)
      val msg = if (job.msg.blocking) {
        CombinedCompletionAndResultMessage(tid, result, instance)
      } else {
        CompletionMessage(tid, result, instance)
      }
      val ack =
        sendActiveAck(tid, result, job.msg.blocking, job.msg.rootControllerIndex, job.msg.user.namespace.uuid, msg)
          .andThen {
            case Failure(e) => logging.error(this, s"failed to send abort ack $e")
          }
      val store = storeActivation(tid, result, job.msg.blocking, context)
        .andThen {
          case Failure(e) => logging.error(this, s"failed to store aborted activation $e")
        }
      //return both futures
      Seq(ack, store)
    }
    Future.sequence(f)
  }

  /**
   * Return any buffered jobs to parent, in case buffer is not empty at removal/error time.
   */
  def rejectBuffered() = {
    //resend any buffered items on container removal
    if (runBuffer.nonEmpty) {
      logging.info(this, s"resending ${runBuffer.size} buffered jobs to parent on container removal")
      runBuffer.foreach(context.parent ! _)
      runBuffer = immutable.Queue.empty[Run]
    }
  }

  private def enableHealthPing(c: Container) = {
    val hpa = healthPingActor.getOrElse {
      logging.info(this, s"creating health ping actor for ${c.addr.asString()}")
      val hp = context.actorOf(
        TCPPingClient
          .props(tcp, c.toString(), healtCheckConfig, new InetSocketAddress(c.addr.host, c.addr.port)))
      healthPingActor = Some(hp)
      hp
    }
    hpa ! HealthPingEnabled(true)
  }

  private def disableHealthPing() = {
    healthPingActor.foreach(_ ! HealthPingEnabled(false))
  }

  /**
   * Runs the job, initialize first if necessary.
   * Completes the job by:
   * 1. sending an activate ack,
   * 2. fetching the logs for the run,
   * 3. indicating the resource is free to the parent pool,
   * 4. recording the result to the data store
   *
   * @param container the container to run the job on
   * @param job       the job to run
   * @return a future completing after logs have been collected and
   *         added to the WhiskActivation
   */
  def initializeAndRun(container: Container, job: Run, reschedule: Boolean = false)(
    implicit tid: TransactionId): Future[WhiskActivation] = {
    val actionTimeout = job.action.limits.timeout.duration
    val unlockedArgs =
      ContainerProxy.unlockArguments(job.msg.content, job.msg.lockedArgs, ParameterEncryption.singleton)

    val (env, parameters) = ContainerProxy.partitionArguments(unlockedArgs, job.msg.initArgs)

    val environment = Map(
      "namespace" -> job.msg.user.namespace.name.toJson,
      "action_name" -> job.msg.action.qualifiedNameWithLeadingSlash.toJson,
      "action_version" -> job.msg.action.version.toJson,
      "activation_id" -> job.msg.activationId.toString.toJson,
      "transaction_id" -> job.msg.transid.id.toJson)

    // if the action requests the api key to be injected into the action context, add it here;
    // treat a missing annotation as requesting the api key for backward compatibility
    val authEnvironment = {
      if (job.action.annotations.isTruthy(Annotations.ProvideApiKeyAnnotationName, valueForNonExistent = true)) {
        job.msg.user.authkey.toEnvironment.fields
      } else Map.empty
    }

    // Only initialize iff we haven't yet warmed the container
    val initialize = stateData match {
      case data: WarmedData =>
        Future.successful(None)
      case _ =>
        val owEnv = (authEnvironment ++ environment ++ Map(
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)) map {
          case (key, value) => "__OW_" + key.toUpperCase -> value
        }

        container
          .initialize(
            job.action.containerInitializer(env ++ owEnv),
            actionTimeout,
            job.action.limits.concurrency.maxConcurrent,
            Some(job.action.toWhiskAction))
          .map(Some(_))
    }

    val activation: Future[WhiskActivation] = initialize
      .flatMap { initInterval =>
        //immediately setup warmedData for use (before first execution) so that concurrent actions can use it asap
        if (initInterval.isDefined) {
          self ! InitCompleted(WarmedData(container, job.msg.user.namespace.name, job.action, Instant.now, 1))
        }

        val env = authEnvironment ++ environment ++ Map(
          // compute deadline on invoker side avoids discrepancies inside container
          // but potentially under-estimates actual deadline
          "deadline" -> (Instant.now.toEpochMilli + actionTimeout.toMillis).toString.toJson)

        container
          .run(
            parameters,
            env.toJson.asJsObject,
            actionTimeout,
            job.action.limits.concurrency.maxConcurrent,
            job.msg.user.limits.allowedMaxPayloadSize,
            job.msg.user.limits.allowedTruncationSize,
            reschedule)(job.msg.transid)
          .map {
            case (runInterval, response) =>
              val initRunInterval = initInterval
                .map(i => Interval(runInterval.start.minusMillis(i.duration.toMillis), runInterval.end))
                .getOrElse(runInterval)
              ContainerProxy.constructWhiskActivation(
                job,
                initInterval,
                initRunInterval,
                runInterval.duration >= actionTimeout,
                response)
          }
      }
      .recoverWith {
        case h: ContainerHealthError =>
          Future.failed(h)
        case InitializationError(interval, response) =>
          Future.successful(
            ContainerProxy
              .constructWhiskActivation(job, Some(interval), interval, interval.duration >= actionTimeout, response))
        case t =>
          // Actually, this should never happen - but we want to make sure to not miss a problem
          logging.error(this, s"caught unexpected error while running activation: ${t}")
          Future.successful(
            ContainerProxy.constructWhiskActivation(
              job,
              None,
              Interval.zero,
              false,
              ActivationResponse.whiskError(Messages.abnormalRun)))
      }

    val splitAckMessagesPendingLogCollection = collectLogs.logsToBeCollected(job.action)
    // Sending an active ack is an asynchronous operation. The result is forwarded as soon as
    // possible for blocking activations so that dependent activations can be scheduled. The
    // completion message which frees a load balancer slot is sent after the active ack future
    // completes to ensure proper ordering.
    val sendResult = if (job.msg.blocking) {
      activation.map { result =>
        val msg =
          if (splitAckMessagesPendingLogCollection) ResultMessage(tid, result)
          else CombinedCompletionAndResultMessage(tid, result, instance)
        sendActiveAck(tid, result, job.msg.blocking, job.msg.rootControllerIndex, job.msg.user.namespace.uuid, msg)
      }
    } else {
      // For non-blocking request, do not forward the result.
      if (splitAckMessagesPendingLogCollection) Future.successful(())
      else
        activation.map { result =>
          val msg = CompletionMessage(tid, result, instance)
          sendActiveAck(tid, result, job.msg.blocking, job.msg.rootControllerIndex, job.msg.user.namespace.uuid, msg)
        }
    }

    val userContext = UserContext(job.msg.user)

    // Adds logs to the raw activation.
    val activationWithLogs: Future[Either[ActivationLogReadingError, WhiskActivation]] = activation
      .flatMap { activation =>
        // Skips log collection entirely, if the limit is set to 0
        if (!splitAckMessagesPendingLogCollection) {
          Future.successful(Right(activation))
        } else {
          val start = tid.started(this, LoggingMarkers.INVOKER_COLLECT_LOGS, logLevel = InfoLevel)
          collectLogs(tid, job.msg.user, activation, container, job.action)
            .andThen {
              case Success(_) => tid.finished(this, start)
              case Failure(t) => tid.failed(this, start, s"reading logs failed: $t")
            }
            .map(logs => Right(activation.withLogs(logs)))
            .recover {
              case LogCollectingException(logs) =>
                Left(ActivationLogReadingError(activation.withLogs(logs)))
              case _ =>
                Left(ActivationLogReadingError(activation.withLogs(ActivationLogs(Vector(Messages.logFailure)))))
            }
        }
      }

    activationWithLogs
      .map(_.fold(_.activation, identity))
      .foreach { activation =>
        // Sending the completion message to the controller after the active ack ensures proper ordering
        // (result is received before the completion message for blocking invokes).
        if (splitAckMessagesPendingLogCollection) {
          sendResult.onComplete(
            _ =>
              sendActiveAck(
                tid,
                activation,
                job.msg.blocking,
                job.msg.rootControllerIndex,
                job.msg.user.namespace.uuid,
                CompletionMessage(tid, activation, instance)))
        }
        storeActivation(tid, activation, job.msg.blocking, userContext)
      }

    // Disambiguate activation errors and transform the Either into a failed/successful Future respectively.
      activationWithLogs.flatMap {
        case Right(act) =>
          // Extract GC metrics from the activation
          val gcMetric = extractGCMetricFromActivation(act)

          // Send updated ContainerData to ContainerPool
          val updatedData = stateData match {
            case data: WarmedData =>
              data.copy(gcMetric = Some(gcMetric))
            case data =>
              data // For other cases, you might need to handle appropriately
          }

          // Send UpdateContainerData message to ContainerPool
          this.context.parent ! UpdateContainerData(updatedData)

          if (act.response.isSuccess || act.response.isApplicationError) {
            if (act.response.isApplicationError && activationErrorLoggingConfig.applicationErrors) {
              logTruncatedError(act)
            }
            Future.successful(act)
          } else {
            if ((act.response.isContainerError && activationErrorLoggingConfig.developerErrors) ||
                (act.response.isWhiskError && activationErrorLoggingConfig.whiskErrors)) {
              logTruncatedError(act)
            }
            Future.failed(ActivationUnsuccessfulError(act))
          }
        case Left(error) => Future.failed(error)
      }
  }

  def extractGCMetricFromActivation(activation: WhiskActivation): GCMetric = {
    // Assuming the GC metrics are returned in the activation's annotations or result
    val annotations = activation.annotations
    val heapAlloc = annotations.get("heapAlloc").map(_.convertTo[Long]).getOrElse(0L)
    val nextGC = annotations.get("nextGC").map(_.convertTo[Long]).getOrElse(Long.MaxValue)
    val numGC = annotations.get("numGC").map(_.convertTo[Long]).getOrElse(0L)

    GCMetric(heapAlloc, nextGC, numGC)
  }

  //to ensure we don't blow up logs with potentially large activation response error
  private def logTruncatedError(act: WhiskActivation) = {
    val truncate = 1024
    val resultString = act.response.result.map(_.compactPrint).getOrElse("[no result]")
    val truncatedResult = if (resultString.length > truncate) {
      s"${resultString.take(truncate)}..."
    } else {
      resultString
    }
    val errorTypeMessage = ActivationResponse.messageForCode(act.response.statusCode)
    logging.warn(
      this,
      s"Activation ${act.activationId} at container ${stateData.getContainer} (with $activeCount still active) returned a $errorTypeMessage: $truncatedResult")
  }
}

final case class ContainerProxyTimeoutConfig(idleContainer: FiniteDuration,
                                             pauseGrace: FiniteDuration,
                                             keepingDuration: FiniteDuration)
final case class ContainerProxyHealthCheckConfig(enabled: Boolean, checkPeriod: FiniteDuration, maxFails: Int)
final case class ContainerProxyActivationErrorLogConfig(applicationErrors: Boolean,
                                                        developerErrors: Boolean,
                                                        whiskErrors: Boolean)

object ContainerProxy {
  def props(factory: (TransactionId,
                      String,
                      ImageName,
                      Boolean,
                      ByteSize,
                      Int,
                      Option[Double],
                      Option[ExecutableWhiskAction]) => Future[Container],
            ack: ActiveAck,
            store: (TransactionId, WhiskActivation, Boolean, UserContext) => Future[Any],
            collectLogs: LogsCollector,
            instance: InvokerInstanceId,
            poolConfig: ContainerPoolConfig,
            healthCheckConfig: ContainerProxyHealthCheckConfig =
              loadConfigOrThrow[ContainerProxyHealthCheckConfig](ConfigKeys.containerProxyHealth),
            activationErrorLogConfig: ContainerProxyActivationErrorLogConfig = activationErrorLogging,
            unusedTimeout: FiniteDuration = timeouts.idleContainer,
            pauseGrace: FiniteDuration = timeouts.pauseGrace,
            tcp: Option[ActorRef] = None) =
    Props(
      new ContainerProxy(
        factory,
        ack,
        store,
        collectLogs,
        instance,
        poolConfig,
        healthCheckConfig,
        activationErrorLogConfig,
        unusedTimeout,
        pauseGrace,
        tcp))

  // Needs to be thread-safe as it's used by multiple proxies concurrently.
  private val containerCount = new Counter

  val timeouts = loadConfigOrThrow[ContainerProxyTimeoutConfig](ConfigKeys.containerProxyTimeouts)
  val activationErrorLogging =
    loadConfigOrThrow[ContainerProxyActivationErrorLogConfig](ConfigKeys.containerProxyActivationErrorLogs)

  /**
   * Generates a unique container name.
   *
   * @param prefix the container name's prefix
   * @param suffix the container name's suffix
   * @return a unique container name
   */
  def containerName(instance: InvokerInstanceId, prefix: String, suffix: String): String = {
    def isAllowed(c: Char): Boolean = c.isLetterOrDigit || c == '_'

    val sanitizedPrefix = prefix.filter(isAllowed)
    val sanitizedSuffix = suffix.filter(isAllowed)

    s"${ContainerFactory.containerNamePrefix(instance)}_${containerCount.next()}_${sanitizedPrefix}_${sanitizedSuffix}"
  }

  /**
   * Creates a WhiskActivation ready to be sent via active ack.
   *
   * @param job the job that was executed
   * @param totalInterval the time it took to execute the job
   * @param response the response to return to the user
   * @return a WhiskActivation to be sent to the user
   */
  def constructWhiskActivation(job: Run,
                               initInterval: Option[Interval],
                               totalInterval: Interval,
                               isTimeout: Boolean,
                               response: ActivationResponse) = {
    val causedBy = if (job.msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    val waitTime = {
      val end = initInterval.map(_.start).getOrElse(totalInterval.start)
      Parameters(WhiskActivation.waitTimeAnnotation, Interval(job.msg.transid.meta.start, end).duration.toMillis.toJson)
    }

    val initTime = {
      initInterval.map(initTime => Parameters(WhiskActivation.initTimeAnnotation, initTime.duration.toMillis.toJson))
    }

    val binding =
      job.msg.action.binding.map(f => Parameters(WhiskActivation.bindingAnnotation, JsString(f.asString)))

    WhiskActivation(
      activationId = job.msg.activationId,
      namespace = job.msg.user.namespace.name.toPath,
      subject = job.msg.user.subject,
      cause = job.msg.cause,
      name = job.action.name,
      version = job.action.version,
      start = totalInterval.start,
      end = totalInterval.end,
      duration = Some(totalInterval.duration.toMillis),
      response = response,
      annotations = {
        Parameters(WhiskActivation.limitsAnnotation, job.action.limits.toJson) ++
          Parameters(WhiskActivation.pathAnnotation, JsString(job.action.fullyQualifiedName(false).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(job.action.exec.kind)) ++
          Parameters(WhiskActivation.timeoutAnnotation, JsBoolean(isTimeout)) ++
          causedBy ++ initTime ++ waitTime ++ binding
      })
  }

  /**
   * Partitions the activation arguments into two JsObject instances. The first is exported as intended for export
   * by the action runtime to the environment. The second is passed on as arguments to the action.
   *
   * @param content the activation arguments
   * @param initArgs set of parameters to treat as initialization arguments
   * @return A partition of the arguments into an environment variables map and the JsObject argument to the action
   */
  def partitionArguments(content: Option[JsValue], initArgs: Set[String]): (Map[String, JsValue], JsValue) = {
    content match {
      case None                                       => (Map.empty, JsObject.empty)
      case Some(JsArray(elements))                    => (Map.empty, JsArray(elements))
      case Some(JsObject(fields)) if initArgs.isEmpty => (Map.empty, JsObject(fields))
      case Some(JsObject(fields)) =>
        val (env, args) = fields.partition(k => initArgs.contains(k._1))
        (env, JsObject(args))
    }
  }

  def unlockArguments(content: Option[JsValue],
                      lockedArgs: Map[String, String],
                      decoder: ParameterEncryption): Option[JsValue] = {
    content match {
      case Some(JsObject(fields)) =>
        Some(JsObject(fields.map {
          case (k, v: JsString) if lockedArgs.contains(k) => (k -> decoder.encryptor(lockedArgs(k)).decrypt(v))
          case p                                          => p
        }))
      // keep the original for other type(e.g. JsArray)
      case contentValue => contentValue
    }
  }
}

object TCPPingClient {
  def props(tcp: ActorRef, containerId: String, config: ContainerProxyHealthCheckConfig, remote: InetSocketAddress) =
    Props(new TCPPingClient(tcp, containerId, remote, config))
}

class TCPPingClient(tcp: ActorRef,
                    containerId: String,
                    remote: InetSocketAddress,
                    config: ContainerProxyHealthCheckConfig)
    extends Actor {
  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec = context.system.dispatcher
  implicit var healthPingTx = TransactionId.actionHealthPing
  case object HealthPingSend

  var scheduledPing: Option[Cancellable] = None
  var failedCount = 0
  val addressString = s"${remote.getHostString}:${remote.getPort}"
  restartPing()

  private def restartPing() = {
    cancelPing() //just in case restart is called twice
    scheduledPing = Some(
      context.system.scheduler.scheduleAtFixedRate(config.checkPeriod, config.checkPeriod, self, HealthPingSend))
  }
  private def cancelPing() = {
    scheduledPing.foreach(_.cancel())
  }
def receive = {
  case HealthPingEnabled(enabled) =>
    if (enabled) {
      restartPing()
    } else {
      cancelPing()
    }
  case HealthPingSend =>
    healthPingTx = TransactionId(systemPrefix + "actionHealth") //reset the tx id each iteration
    tcp ! Connect(remote)
  case CommandFailed(_: Connect) =>
    failedCount += 1
    if (failedCount == config.maxFails) {
      logging.error(
        this,
        s"Failed health connection to $containerId ($addressString) $failedCount times - exceeded max ${config.maxFails} failures")
      //destroy this container since we cannot communicate with it
      context.parent ! FailureMessage(
        new SocketException(s"Health connection to $containerId ($addressString) failed $failedCount times"))
      cancelPing()
      context.stop(self)
    } else {
      logging.warn(this, s"Failed health connection to $containerId ($addressString) $failedCount times")
    }

  case Connected(_, _) =>
    sender() ! Close
    if (failedCount > 0) {
      //reset in case of temp failure
      logging.info(
        this,
        s"Succeeded health connection to $containerId ($addressString) after $failedCount previous failures")
      failedCount = 0
    } else {
      logging.debug(this, s"Succeeded health connection to $containerId ($addressString)")
    }
  }
}

/** Indicates that something went wrong with an activation and the container should be removed */
trait ActivationError extends Exception {
  val activation: WhiskActivation
}

/** Indicates an activation with a non-successful response */
case class ActivationUnsuccessfulError(activation: WhiskActivation) extends ActivationError

/** Indicates reading logs for an activation failed (terminally, truncated) */
case class ActivationLogReadingError(activation: WhiskActivation) extends ActivationError

 */