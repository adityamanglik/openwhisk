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

  // Scheduling Policy
  val schedulingPolicy: String = "GCMitigation" // Changed to "GCMitigation"
  var initialRunMessage: Option[Run] = None
  val numContainers: Int = 2 // Number of containers for RoundRobin policy

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

  // Added for GCMitigation policy
  val requestHeapMargin: Long = 5 * 1024 * 1024L // Configurable margin

  def receive: Receive = {
    // A job to run on a container
    //
    // Run messages are received either via the feed or from child containers which cannot process
    // their requests and send them back to the pool for rescheduling (this may happen if "docker" operations
    // fail for example, or a container has aged and was destroying itself when a new request was assigned)
    case r: Run =>
      // Capture the initial Run message
      if (initialRunMessage.isEmpty) {
        initialRunMessage = Some(r)
        logging.info(this, s"Captured initial Run message for action: ${r.action.fullyQualifiedName(false)}")
      }
      val actionName = r.action.fullyQualifiedName(false)
      schedulingPolicy match {
        case "GCMitigation" =>
          actionContainers.get(actionName) match {
            case Some(RoundRobinContainerData(containers, nextIndex)) =>
              // Find a container that is not about to undergo GC
              val suitableContainerOption = containers.find { case (_, data) =>
                data match {
                  case warmedData: WarmedData =>
                    warmedData.gcMetric match {
                      case Some(gcMetric) =>
                        val heapAlloc = gcMetric.heapAlloc
                        val nextGC = gcMetric.nextGC
                        val margin = requestHeapMargin
                        (heapAlloc + margin) <= nextGC
                      case None =>
                        true //  or false, depending on your policy
                    }
                  case _ =>
                    true // If not WarmedData, consider it suitable
                }
              }

              suitableContainerOption match {
                case Some((actorRef, containerData)) =>
                  // Found a suitable container
                  val newData = containerData.nextRun(r)
                  val index = containers.indexWhere { case (ref, _) => ref == actorRef }
                  if (index >= 0) {
                    val updatedContainers = containers.updated(index, (actorRef, newData))
                    val updatedNextIndex = (nextIndex + 1) % containers.size
                    actionContainers = actionContainers.updated(actionName, RoundRobinContainerData(updatedContainers, updatedNextIndex))
                    actorRef ! r
                    logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
                    // Move container from freePool to busyPool
                    freePool = freePool - actorRef
                    busyPool = busyPool + (actorRef -> newData)
                    resent = None
                  } else {
                    logging.warn(this, s"Could not find container $actorRef in containers list")
                  }
                case None =>
                  // All containers are close to GC, send fake activation to trigger GC
                  val activationRequest = createModifiedRun(r)
                  containers.foreach { case (actorRef, containerData) =>
                    actorRef ! activationRequest
                  }
                  // Buffer the request
                  runBuffer = runBuffer.enqueue(r)
                  logging.info(this, s"All containers are undergoing GC; buffering request.")
              }
            
            case Some(SingleContainerData((actorRef, containerData))) =>
              // Handle unexpected SingleContainerData in GCMitigation policy
              logging.warn(this, s"Expected RoundRobinContainerData but found SingleContainerData for action $actionName")
              // Convert SingleContainerData to RoundRobinContainerData with a single container
              val containers = List((actorRef, containerData))
              val updatedContainers = containers
              val updatedNextIndex = 0
              actionContainers = actionContainers.updated(
                actionName,
                RoundRobinContainerData(updatedContainers, updatedNextIndex))
              self ! r // Retry processing the Run message

            case None =>
              // Create initial containers
              val memory = r.action.limits.memory.megabytes.MB
              val newContainers = (1 to numContainers).map { _ =>
                createContainer(memory)
              }.toList
              val updatedContainers = newContainers.map { case (actorRef, containerData) => (actorRef, containerData) }
              val (actorRef, containerData) = updatedContainers(0)
              val newData = containerData.nextRun(r)
              val updatedContainersWithNewData = updatedContainers.updated(0, (actorRef, newData))
              actionContainers = actionContainers + (actionName -> RoundRobinContainerData(updatedContainersWithNewData, 1))
              actorRef ! r
              logContainerStart(r, "cold", newData.activeActivationCount, newData.getContainer)
              // Add this code to move container from freePool to busyPool
              freePool = freePool - actorRef
              busyPool = busyPool + (actorRef -> newData)
          }

        case "SingleContainer" =>
          actionContainers.get(actionName) match {
            case Some(SingleContainerData((actorRef, containerData))) =>
              val newData = containerData.nextRun(r)
              actionContainers = actionContainers + (actionName -> SingleContainerData((actorRef, newData)))
              actorRef ! r
              logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
              resent = None
            case Some(otherData) =>
              logging.warn(this, s"Expected SingleContainerData but found $otherData")
            case None =>
              // Create only one new container
              val memory = r.action.limits.memory.megabytes.MB
              val container = createContainer(memory)
              val (actorRef, containerData) = container
              val newData = containerData.nextRun(r)
              actionContainers = actionContainers + (actionName -> SingleContainerData((actorRef, newData)))
              actorRef ! r
              logContainerStart(r, "cold", newData.activeActivationCount, newData.getContainer)
          }

        case "RoundRobin" =>
          actionContainers.get(actionName) match {
            case Some(RoundRobinContainerData(containers, nextIndex)) =>
              val (actorRef, containerData) = containers(nextIndex)
              val newData = containerData.nextRun(r)
              val updatedContainers = containers.updated(nextIndex, (actorRef, newData))
              val updatedNextIndex = (nextIndex + 1) % containers.size
              actionContainers =
                actionContainers.updated(actionName, RoundRobinContainerData(updatedContainers, updatedNextIndex))
              actorRef ! r
              logContainerStart(r, "existing", newData.activeActivationCount, newData.getContainer)
              resent = None
            case Some(otherData) =>
              logging.warn(this, s"Expected RoundRobinContainerData but found $otherData")
            case None =>
              // Create multiple containers based on numContainers
              val memory = r.action.limits.memory.megabytes.MB
              val newContainers = (1 to numContainers).map { _ =>
                createContainer(memory)
              }.toList
              val updatedContainers = newContainers.map { case (actorRef, containerData) => (actorRef, containerData) }
              // Start with the first container
              val (actorRef, containerData) = updatedContainers(0)
              val newData = containerData.nextRun(r)
              val updatedContainersWithNewData = updatedContainers.updated(0, (actorRef, newData))
              actionContainers = actionContainers + (actionName -> RoundRobinContainerData(
                updatedContainersWithNewData,
                1))
              actorRef ! r
              logContainerStart(r, "cold", newData.activeActivationCount, newData.getContainer)
          }

        case _ =>
          logging.error(this, s"Unknown scheduling policy: $schedulingPolicy")
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
