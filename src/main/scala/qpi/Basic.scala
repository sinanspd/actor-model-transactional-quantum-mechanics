package com.sinanspd.qpi

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

import scala.concurrent.duration.*
import scala.util.Random
import scala.concurrent.Await

object Spacetime {
  sealed trait Command

  final case class SendOffer(
    emissionId: Long,
    emitter: ActorRef[Emitter.Command],
    emitterPos: Vec2,
    offerAmp: Complex,
    waveNumberK: Double,
    to: ActorRef[Receiver.Command],
    toPos: Vec2
  ) extends Command

  final case class SendConfirmation(
    emissionId: Long,
    receiver: ActorRef[Receiver.Command],
    receiverPos: Vec2,
    responseAmp: Complex,
    confirmationAmp: Complex,
    to: ActorRef[Emitter.Command],
    toPos: Vec2
  ) extends Command

  def apply(speed: Double = 0.04, jitterMs: Double = 3.0): Behavior[Command] =
    Behaviors.setup { ctx =>
      val rng = new Random(1)

      def delayMs(from: Vec2, to: Vec2): Double = {
        val d = from.dist(to)
        val base = d / speed
        val jitter = rng.nextDouble() * jitterMs
        math.max(0.0, base + jitter)
      }

      def asFiniteDelay(ms: Double): FiniteDuration =
        ms.round.millis 

      Behaviors.receiveMessage {
        case SendOffer(emissionId, emitter, emitterPos, offerAmp, k, to, toPos) =>
          val ms = delayMs(emitterPos, toPos)
          val d  = asFiniteDelay(ms)
          ctx.log.info(s"[Spacetime] offer  emission=$emissionId -> ${to.path.name} in ${d.toMillis}ms")
          ctx.scheduleOnce(d, to, Receiver.Offer(emissionId, emitter, emitterPos, offerAmp, k))
          Behaviors.same

        case SendConfirmation(emissionId, receiver, receiverPos, responseAmp, confirmationAmp, to, toPos) =>
          val ms = delayMs(receiverPos, toPos)
          val d  = asFiniteDelay(ms)
          ctx.log.info(s"[Spacetime] confirm emission=$emissionId -> ${to.path.name} from ${receiver.path.name} in ${d.toMillis}ms")
          ctx.scheduleOnce(d, to, Emitter.ConfirmationArrived(emissionId, receiver, responseAmp, confirmationAmp))
          Behaviors.same
      }
    }
}

object Receiver {
  sealed trait Command

  final case class Offer(
    emissionId: Long,
    emitter: ActorRef[Emitter.Command],
    emitterPos: Vec2,
    offerAmpAtSource: Complex,
    waveNumberK: Double
  ) extends Command

  final case class Commit(emissionId: Long, emitter: ActorRef[Emitter.Command]) extends Command
  final case class Abort(emissionId: Long) extends Command

  private def propagationFactor(distance: Double, k: Double): Complex = {
    val atten = 1.0 / math.sqrt(distance + 1.0)
    val phase = k * distance
    Complex(atten * math.cos(phase), atten * math.sin(phase))
  }

  final case class Params(
    name: String,
    pos: Vec2,
    coupling: Complex // receiver-specific 
  )

  def apply(params: Params, spacetime: ActorRef[Spacetime.Command]): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case Offer(emissionId, emitter, emitterPos, offerAtSource, k) =>
          val r = params.pos.dist(emitterPos)
          val offerHere = offerAtSource * propagationFactor(r, k)
          val responseAmp = offerHere * params.coupling
          val confirmationAmp = responseAmp.conj // advanced wave

          // Propagate the confirmation wave backwards through time:
          ctx.log.info(s"[${params.name}] sending confirmation for emission=$emissionId to spacetime")
          spacetime ! Spacetime.SendConfirmation(
            emissionId = emissionId,
            receiver = ctx.self,
            receiverPos = params.pos,
            responseAmp = responseAmp,
            confirmationAmp = confirmationAmp,
            to = emitter,
            toPos = emitterPos
          )

          ctx.log.debug(f"[${params.name}] Offer received. r=$r%.3f offerHere=$offerHere response=$responseAmp confirm=$confirmationAmp")
          Behaviors.same

        case Commit(emissionId, emitter) =>
          ctx.log.info(s"[${params.name}] TRANSACTION COMMIT for emission=$emissionId (absorption event).")
          emitter ! Emitter.AbsorptionAck(emissionId, ctx.self, params.name)
          Behaviors.same

        case Abort(emissionId) =>
          ctx.log.debug(s"[${params.name}] Transaction abort for emission=$emissionId.")
          Behaviors.same
      }
    }
}

object Emitter {
  sealed trait Command

  final case class ReceiverInfo(name: String, ref: ActorRef[Receiver.Command], pos: Vec2)

  final case class Emit(
    emissionId: Long,
    emitterPos: Vec2,
    offerAmpAtSource: Complex,
    waveNumberK: Double,
    receivers: Vector[ReceiverInfo],
    observer: ActorRef[Simulation.Command]
  ) extends Command

  final case class ConfirmationArrived(
    emissionId: Long,
    receiver: ActorRef[Receiver.Command],
    responseAmp: Complex,
    confirmationAmp: Complex
  ) extends Command

  final case class AbsorptionAck(emissionId: Long, receiver: ActorRef[Receiver.Command], receiverName: String) extends Command

  private final case class DecisionTimeout(emissionId: Long) extends Command
  private final case class AckTimeout(emissionId: Long) extends Command

  private final case class InFlight(
    emissionId: Long,
    emitterPos: Vec2,
    offerAmpAtSource: Complex,
    waveNumberK: Double,
    receivers: Vector[ReceiverInfo],
    observer: ActorRef[Simulation.Command],
    confirmations: Map[ActorRef[Receiver.Command], Complex], // receiver -> responseAmp
    decided: Option[ReceiverInfo],
    probs: Vector[(String, Double)]
  )

  def apply(spacetime: ActorRef[Spacetime.Command]): Behavior[Command] =
    Behaviors.setup { ctx =>
      val rng = new Random(42)

      def weightedChoice(items: Vector[(ReceiverInfo, Double)]): Option[ReceiverInfo] = {
        val cleaned = items.filter(_._2 > 0.0)
        val total = cleaned.map(_._2).sum
        if (cleaned.isEmpty || total <= 0.0) None
        else {
          val x = rng.nextDouble() * total
          var acc = 0.0
          cleaned.find { case (_, w) =>
            acc += w
            acc >= x
          }.map(_._1)
        }
      }

      def idle: Behavior[Command] =
        Behaviors.receiveMessage {
          case e @ Emit(emissionId, emitterPos, offerAmpAtSource, k, receivers, observer) =>
            ctx.log.info(s"[Emitter] Emission $emissionId: broadcasting offer wave to ${receivers.size} receivers.")
            receivers.foreach { ri =>
              spacetime ! Spacetime.SendOffer(
                emissionId = emissionId,
                emitter = ctx.self,
                emitterPos = emitterPos,
                offerAmp = offerAmpAtSource,
                waveNumberK = k,
                to = ri.ref,
                toPos = ri.pos
              )
            }

            val timeout = 1.second
            ctx.log.info(s"[Emitter] Emission $emissionId: scheduling decision timeout in ${timeout.toMillis}ms")
            ctx.scheduleOnce(timeout, ctx.self, DecisionTimeout(emissionId))

            inFlight(
              InFlight(
                emissionId = emissionId,
                emitterPos = emitterPos,
                offerAmpAtSource = offerAmpAtSource,
                waveNumberK = k,
                receivers = receivers,
                observer = observer,
                confirmations = Map.empty,
                decided = None,
                probs = Vector.empty
              )
            )

          case other =>
            ctx.log.warn(s"[Emitter] Unexpected in idle: $other")
            Behaviors.same
        }

      def inFlight(st: InFlight): Behavior[Command] =
        Behaviors.receiveMessage {
          case ConfirmationArrived(emissionId, receiverRef, responseAmp, _)
              if emissionId == st.emissionId && st.decided.isEmpty =>

            ctx.log.info(s"[Emitter] Emission $emissionId: confirmation arrived from ${receiverRef.path.name}, |resp|^2=${responseAmp.abs2}")
            val updated = st.confirmations.updated(receiverRef, responseAmp)

            val haveAll = updated.size >= st.receivers.size
            if (haveAll) decideAndCommit(st.copy(confirmations = updated), reason = "all confirmations received")
            else inFlight(st.copy(confirmations = updated))

          case DecisionTimeout(emissionId) if emissionId == st.emissionId && st.decided.isEmpty =>
            ctx.log.info(s"[Emitter] Emission $emissionId: decision timeout; confirmations=${st.confirmations.size}/${st.receivers.size}")
            decideAndCommit(st, reason = "decision timeout")

          case AbsorptionAck(emissionId, _, receiverName) if emissionId == st.emissionId && st.decided.nonEmpty =>
            ctx.log.info(s"[Emitter] Emission $emissionId: absorption ACK from $receiverName. Collapse finalized.")
            st.observer ! Simulation.Collapsed(emissionId, receiverName, st.probs)
            idle

          case AckTimeout(emissionId) if emissionId == st.emissionId && st.decided.nonEmpty =>
            ctx.log.warn(s"[Emitter] Emission $emissionId: no absorption ACK received before timeout; finalizing anyway.")
            val chosenName = st.decided.map(_.name).getOrElse("UNKNOWN")
            st.observer ! Simulation.Collapsed(emissionId, chosenName, st.probs)
            idle

          case other =>
            ctx.log.debug(s"[Emitter] Ignoring/Unexpected while in-flight: $other")
            Behaviors.same
        }

      def decideAndCommit(st: InFlight, reason: String): Behavior[Command] = {
        val weights: Vector[(ReceiverInfo, Double)] =
          st.receivers.flatMap { ri =>
            st.confirmations.get(ri.ref).map(resp => ri -> resp.abs2)
          }

        if (weights.isEmpty) {
          ctx.log.warn(s"[Emitter] Emission ${st.emissionId}: no confirmations ($reason). No transaction.")
          st.observer ! Simulation.NoTransaction(st.emissionId)
          idle
        } else {
          val total = weights.map(_._2).sum
          val probs = st.receivers.map { ri =>
            val w = weights.find(_._1.ref == ri.ref).map(_._2).getOrElse(0.0)
            ri.name -> (if (total > 0.0) w / total else 0.0)
          }

          val chosenOpt = weightedChoice(weights)
          val chosen = chosenOpt.getOrElse(weights.maxBy(_._2)._1)

          ctx.log.info(s"[Emitter] Emission ${st.emissionId}: collapse decision ($reason) => ${chosen.name}")
          st.receivers.foreach { ri =>
            if (ri.ref == chosen.ref) ri.ref ! Receiver.Commit(st.emissionId, ctx.self)
            else ri.ref ! Receiver.Abort(st.emissionId)
          }

          ctx.scheduleOnce(200.millis, ctx.self, AckTimeout(st.emissionId))

          inFlight(st.copy(decided = Some(chosen), probs = probs))
        }
      }

      idle
    }
}

object Simulation {
  sealed trait Command
  private case object Start extends Command

  final case class Collapsed(emissionId: Long, chosenReceiver: String, probs: Vector[(String, Double)]) extends Command
  final case class NoTransaction(emissionId: Long) extends Command

  def apply(shots: Int = 500): Behavior[Command] =
    Behaviors.setup { ctx =>
      val spacetime = ctx.spawn(Spacetime(speed = 0.05, jitterMs = 2.0), "spacetime")
      val emitter = ctx.spawn(Emitter(spacetime), "emitter")

      val emitterPos = Vec2(0.0, 0.0)
      val k = 2.0 * math.Pi / 6.0 

      val r1Params = Receiver.Params("R1", pos = Vec2(3.0, 1.0), coupling = Complex(1.00, 0.00))
      val r2Params = Receiver.Params("R2", pos = Vec2(6.0, 0.0), coupling = Complex(1.30, 0.00))
      val r3Params = Receiver.Params("R3", pos = Vec2(2.0, 5.0), coupling = Complex(0.80, 0.20))

      val r1 = ctx.spawn(Receiver(r1Params, spacetime), "R1")
      val r2 = ctx.spawn(Receiver(r2Params, spacetime), "R2")
      val r3 = ctx.spawn(Receiver(r3Params, spacetime), "R3")

      val receivers = Vector(
        Emitter.ReceiverInfo("R1", r1, r1Params.pos),
        Emitter.ReceiverInfo("R2", r2, r2Params.pos),
        Emitter.ReceiverInfo("R3", r3, r3Params.pos)
      )

      def loop(remaining: Int, nextId: Long, counts: Map[String, Int], noTx: Int): Behavior[Command] =
        Behaviors.receiveMessage {
          case Start =>
            emitter ! Emitter.Emit(
              emissionId = nextId,
              emitterPos = emitterPos,
              offerAmpAtSource = Complex(1.0, 0.0),
              waveNumberK = k,
              receivers = receivers,
              observer = ctx.self
            )
            loop(remaining, nextId, counts, noTx)

          case Collapsed(emissionId, chosen, probs) =>
            val updated = counts.updated(chosen, counts.getOrElse(chosen, 0) + 1)
            val rem = remaining - 1

            if (emissionId <= 5) {
              ctx.log.info(s"[Sim] Emission=$emissionId probabilities: " + probs.map { case (n, p) => f"$n:$p%.3f" }.mkString(", "))
            }

            if (rem <= 0) {
              val total = updated.values.sum.toDouble
              ctx.log.info(s"[Sim] Completed $shots shots. No-transaction=$noTx.")
              updated.toVector.sortBy(_._1).foreach { case (name, c) =>
                val pct = if (total > 0) c / total else 0.0
                ctx.log.info(f"[Sim] $name -> $c ($pct%.3f)")
              }
              Behaviors.stopped
            } else {
              // Fire next emission
              val newId = nextId + 1
              emitter ! Emitter.Emit(
                emissionId = newId,
                emitterPos = emitterPos,
                offerAmpAtSource = Complex(1.0, 0.0),
                waveNumberK = k,
                receivers = receivers,
                observer = ctx.self
              )
              loop(rem, newId, updated, noTx)
            }

          case NoTransaction(emissionId) =>
            ctx.log.warn(s"[Sim] Emission=$emissionId produced no transaction.")
            val rem = remaining - 1
            if (rem <= 0) Behaviors.stopped
            else {
              val newId = nextId + 1
              emitter ! Emitter.Emit(
                emissionId = newId,
                emitterPos = emitterPos,
                offerAmpAtSource = Complex(1.0, 0.0),
                waveNumberK = k,
                receivers = receivers,
                observer = ctx.self
              )
              loop(rem, newId, counts, noTx + 1)
            }
        }

      ctx.self ! Start
      loop(remaining = shots, nextId = 1L, counts = Map.empty, noTx = 0)
    }
}

object TransactionalBasic{
  def main(args: Array[String]): Unit = {
    val shots = args.headOption.flatMap(_.toIntOption).getOrElse(5)
    val system: ActorSystem[Simulation.Command] = ActorSystem(Simulation(shots), "ti-demo")
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
