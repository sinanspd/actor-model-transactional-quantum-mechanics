import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors, TimerScheduler}

import scala.concurrent.Await
import scala.concurrent.duration.*
import scala.util.Random

import com.sinanspd.qpi._

enum Side { case Left, Right }
enum Outcome(val value: Int) { case Plus extends Outcome(+1); case Minus extends Outcome(-1) }

object Spacetime {
  sealed trait Command

  final case class SendOffer(
    emissionId: Long,
    sourceId: String,
    sourcePos: Vec2,
    k: Double,
    offerAmp: Complex,
    to: ActorRef[Receiver.Command],
    toPos: Vec2,
    manager: ActorRef[TransactionManager.Command]
  ) extends Command

  final case class SendConfirmationSingle(
    emissionId: Long,
    from: ActorRef[Receiver.Command],
    responseAmp: Complex,
    to: ActorRef[TransactionManager.Command]
  ) extends Command

  final case class SendReadyBell(
    emissionId: Long,
    from: ActorRef[BellDetector.Command],
    side: Side,
    outcome: Outcome,
    theta: Double,
    to: ActorRef[TransactionManager.Command]
  ) extends Command

  def apply(speed: Double, jitterMs: Double, seed: Long): Behavior[Command] =
    Behaviors.setup { ctx =>
      val rng = new Random(seed)

      def delayMs(from: Vec2, to: Vec2): Long = {
        val d = from.dist(to)
        val base = d / speed
        val jitter = rng.nextDouble() * jitterMs
        math.max(0.0, base + jitter).round
      }

      def asFiniteDelay(ms: Double): FiniteDuration =
        ms.round.millis 

      Behaviors.receiveMessage {
        case SendOffer(emissionId, sourceId, sourcePos, k, baseAmp, to, toPos, manager) =>
          val ms = delayMs(sourcePos, toPos)
          ctx.log.info(s"[Spacetime] offer  emission=$emissionId -> ${to.path.name} in ${asFiniteDelay(ms.toDouble).toMillis}ms")
          ctx.scheduleOnce(ms.millis, to, Receiver.OfferSingle(emissionId, sourceId, sourcePos, k, baseAmp, manager))
          Behaviors.same

        case SendConfirmationSingle(emissionId, from, responseAmp, to) =>
          ctx.scheduleOnce(0.millis, to, TransactionManager.SingleConfirmation(emissionId, from, responseAmp))
          Behaviors.same

        case SendReadyBell(emissionId, from, side, outcome, theta, to) =>
          ctx.scheduleOnce(0.millis, to, TransactionManager.BellReady(emissionId, from, side, outcome, theta))
          Behaviors.same
      }
    }
}

// Agg multiple offers coherently and returns one confirmation
object Receiver {
  sealed trait Command

  final case class OfferSingle(
    emissionId: Long,
    sourceId: String,
    sourcePos: Vec2,
    k: Double,
    baseAmp: Complex,
    manager: ActorRef[TransactionManager.Command]
  ) extends Command

  private final case class FinalizeSingle(emissionId: Long) extends Command

  final case class CommitSingle(emissionId: Long) extends Command
  final case class AbortSingle(emissionId: Long) extends Command

  final case class Params(
    name: String,
    pos: Vec2,
    coupling: Complex,
    coherenceWindow: FiniteDuration
  )

  // attenuation * phase rotation as function of distance
  private def propagation(distance: Double, k: Double): Complex = {
    val atten = 1.0 / math.sqrt(distance + 1.0)
    val phase = k * distance
    Complex(atten * math.cos(phase), atten * math.sin(phase))
  }

  private final case class InFlightSingle(
    manager: ActorRef[TransactionManager.Command],
    offers: Map[String, Complex] 
  )

  def apply(params: Params, spacetime: ActorRef[Spacetime.Command]): Behavior[Command] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { ctx =>
        var inflight: Map[Long, InFlightSingle] = Map.empty

        def finalizeEmission(emissionId: Long): Unit = {
          inflight.get(emissionId).foreach { st =>
            val aTotal: Complex = st.offers.values.foldLeft(Complex.zero)(_ + _)
            val responseAmp = aTotal * params.coupling
            spacetime ! Spacetime.SendConfirmationSingle(emissionId, ctx.self, responseAmp, st.manager)
          }
          inflight -= emissionId
        }

        Behaviors.receiveMessage {
          case OfferSingle(emissionId, sourceId, sourcePos, k, baseAmp, manager) =>
            val dist = params.pos.dist(sourcePos)
            val ampHere = baseAmp * propagation(dist, k) //advanced wave amp

            inflight.get(emissionId) match {
              case None =>
                inflight += emissionId -> InFlightSingle(manager, Map(sourceId -> ampHere))
                timers.startSingleTimer(FinalizeSingle(emissionId), params.coherenceWindow)
              case Some(st) =>
                inflight += emissionId -> st.copy(offers = st.offers.updated(sourceId, ampHere))
            }
            Behaviors.same

          case FinalizeSingle(emissionId) =>
            finalizeEmission(emissionId)
            Behaviors.same

          case CommitSingle(emissionId) =>
            ctx.log.info(s"[${params.name}] COMMIT emission=$emissionId (absorption)")
            Behaviors.same

          case AbortSingle(_) =>
            Behaviors.same
        }
      }
    }
}

// -------------------------
// Bell detectors (absorbers) for joint transaction commits
// -------------------------
object BellDetector {
  sealed trait Command
  final case class Arm(emissionId: Long, theta: Double, manager: ActorRef[TransactionManager.Command]) extends Command
  final case class CommitBell(emissionId: Long) extends Command
  final case class AbortBell(emissionId: Long) extends Command

  final case class Params(name: String, side: Side, outcome: Outcome)

  def apply(params: Params, spacetime: ActorRef[Spacetime.Command]): Behavior[Command] =
    Behaviors.setup { ctx =>
      Behaviors.receiveMessage {
        case Arm(emissionId, theta, manager) =>
          spacetime ! Spacetime.SendReadyBell(emissionId, ctx.self, params.side, params.outcome, theta, manager)
          Behaviors.same

        case CommitBell(emissionId) =>
          ctx.log.info(s"[${params.name}] COMMIT bell emission=$emissionId outcome=${params.outcome.value}")
          Behaviors.same

        case AbortBell(_) =>
          Behaviors.same
      }
    }
}

// -------------------------
// TransactionManager
// - Single: choose one receiver with probability proportional to |response|^2
// - Bell: choose a joint pair (left outcome, right outcome) using singlet correlations
// -------------------------
object TransactionManager {
  sealed trait Command

  // ---- Single transaction protocol ----
  final case class BeginSingle(
    emissionId: Long,
    receivers: Vector[ActorRef[Receiver.Command]],
    replyTo: ActorRef[SimulationController.Command],
    timeout: FiniteDuration
  ) extends Command

  final case class SingleConfirmation(emissionId: Long, from: ActorRef[Receiver.Command], responseAmp: Complex) extends Command
  private final case class SingleTimeout(emissionId: Long) extends Command

  // ---- Bell / joint protocol ----
  final case class BeginBell(
    emissionId: Long,
    thetaL: Double,
    thetaR: Double,
    left: Vector[ActorRef[BellDetector.Command]],
    right: Vector[ActorRef[BellDetector.Command]],
    replyTo: ActorRef[SimulationController.Command],
    timeout: FiniteDuration
  ) extends Command

  final case class BellReady(
    emissionId: Long,
    from: ActorRef[BellDetector.Command],
    side: Side,
    outcome: Outcome,
    theta: Double
  ) extends Command

  private final case class BellTimeout(emissionId: Long) extends Command

  private final case class SingleState(
    receivers: Vector[ActorRef[Receiver.Command]],
    replyTo: ActorRef[SimulationController.Command],
    confirmations: Map[ActorRef[Receiver.Command], Complex]
  )

  private final case class BellState(
    thetaL: Double,
    thetaR: Double,
    left: Vector[ActorRef[BellDetector.Command]],
    right: Vector[ActorRef[BellDetector.Command]],
    replyTo: ActorRef[SimulationController.Command],
    readyL: Map[Outcome, ActorRef[BellDetector.Command]],
    readyR: Map[Outcome, ActorRef[BellDetector.Command]]
  )

  def apply(seed: Long): Behavior[Command] =
    Behaviors.withTimers { timers =>
      Behaviors.setup { ctx =>
        val rng = new Random(seed)

        var single: Map[Long, SingleState] = Map.empty
        var bell: Map[Long, BellState] = Map.empty

        def weightedChoice(items: Vector[(ActorRef[Receiver.Command], Double)]): ActorRef[Receiver.Command] = {
          val cleaned = items.filter(_._2 > 0.0)
          val total = cleaned.map(_._2).sum
          if (cleaned.isEmpty || total <= 0.0) cleaned.headOption.map(_._1).getOrElse(items.head._1)
          else {
            val x = rng.nextDouble() * total
            var acc = 0.0
            cleaned.find { case (_, w) => acc += w; acc >= x }
              .map(_._1)
              .getOrElse(cleaned.maxBy(_._2)._1)
          }
        }

        def decideSingle(emissionId: Long): Unit = {
          single.get(emissionId).foreach { st =>
            val weights = st.confirmations.toVector.map { case (r, amp) => (r, amp.abs2) }
            if (weights.isEmpty) {
              st.replyTo ! SimulationController.SingleNoTransaction(emissionId)
            } else {
              val total = weights.map(_._2).sum
              val probs = st.receivers.map { r =>
                val w = weights.find(_._1 == r).map(_._2).getOrElse(0.0)
                r -> (if (total > 0.0) w / total else 0.0)
              }

              val chosen = weightedChoice(weights)
              st.receivers.foreach { r =>
                if (r == chosen) r ! Receiver.CommitSingle(emissionId)
                else r ! Receiver.AbortSingle(emissionId)
              }
              st.replyTo ! SimulationController.SingleCollapsed(emissionId, chosen, probs)
            }
          }
          single -= emissionId
        }

        def decideBell(emissionId: Long): Unit = {
          bell.get(emissionId).foreach { st =>
            val haveAll = st.readyL.size == 2 && st.readyR.size == 2
            if (!haveAll) {
              st.replyTo ! SimulationController.BellNoTransaction(emissionId)
              bell -= emissionId
            } else {
              val delta = st.thetaL - st.thetaR
              val c = math.cos(delta)

              // Singlet: P(a,b) = 1/4(1 - a*b*cos(delta))
              // P(same) = (1 - cos(delta))/2, P(opposite) = (1 + cos(delta))/2
              val pSame = (1.0 - c) / 2.0
              val same = rng.nextDouble() < pSame

              val (a, b) =
                if (same) {
                  if (rng.nextBoolean()) (Outcome.Plus, Outcome.Plus) else (Outcome.Minus, Outcome.Minus)
                } else {
                  if (rng.nextBoolean()) (Outcome.Plus, Outcome.Minus) else (Outcome.Minus, Outcome.Plus)
                }

              val leftDet  = st.readyL(a)
              val rightDet = st.readyR(b)

              st.left.foreach(d => if (d == leftDet) d ! BellDetector.CommitBell(emissionId) else d ! BellDetector.AbortBell(emissionId))
              st.right.foreach(d => if (d == rightDet) d ! BellDetector.CommitBell(emissionId) else d ! BellDetector.AbortBell(emissionId))

              st.replyTo ! SimulationController.BellCollapsed(emissionId, st.thetaL, st.thetaR, a, b)
              bell -= emissionId
            }
          }
        }

        Behaviors.receiveMessage {
          // ---- single ----
          case BeginSingle(emissionId, receivers, replyTo, timeout) =>
            single += emissionId -> SingleState(receivers, replyTo, Map.empty)
            timers.startSingleTimer(SingleTimeout(emissionId), timeout)
            Behaviors.same

          case SingleConfirmation(emissionId, from, responseAmp) =>
            single.get(emissionId) match {
              case None => Behaviors.same
              case Some(st) =>
                val st2 = st.copy(confirmations = st.confirmations.updated(from, responseAmp))
                single += emissionId -> st2
                if (st2.confirmations.size >= st2.receivers.size) decideSingle(emissionId)
                Behaviors.same
            }

          case SingleTimeout(emissionId) =>
            decideSingle(emissionId)
            Behaviors.same

          // ---- bell ----
          case BeginBell(emissionId, thetaL, thetaR, left, right, replyTo, timeout) =>
            bell += emissionId -> BellState(thetaL, thetaR, left, right, replyTo, Map.empty, Map.empty)
            timers.startSingleTimer(BellTimeout(emissionId), timeout)
            Behaviors.same

          case BellReady(emissionId, from, side, outcome, _) =>
            bell.get(emissionId) match {
              case None => Behaviors.same
              case Some(st) =>
                val st2 =
                  side match {
                    case Side.Left  => st.copy(readyL = st.readyL.updated(outcome, from))
                    case Side.Right => st.copy(readyR = st.readyR.updated(outcome, from))
                  }
                bell += emissionId -> st2
                if (st2.readyL.size == 2 && st2.readyR.size == 2) decideBell(emissionId)
                Behaviors.same
            }

          case BellTimeout(emissionId) =>
            decideBell(emissionId)
            Behaviors.same
        }
      }
    }
}

object SimulationController {
  sealed trait Command
  private case object Start extends Command

  final case class SingleCollapsed(
    emissionId: Long,
    chosen: ActorRef[Receiver.Command],
    probs: Vector[(ActorRef[Receiver.Command], Double)]
  ) extends Command

  final case class SingleNoTransaction(emissionId: Long) extends Command

  final case class BellCollapsed(emissionId: Long, thetaL: Double, thetaR: Double, a: Outcome, b: Outcome) extends Command
  final case class BellNoTransaction(emissionId: Long) extends Command

  final case class Config(
    mode: String = "teach",     
    shots: Int = 2000,          
    bins: Int = 61,             
    seed: Long = 7L
  )

  def parseArgs(args: Array[String]): Config = {
    val kv = args.toVector.flatMap { a =>
      if (a.startsWith("--") && a.contains("=")) {
        val Array(k, v) = a.drop(2).split("=", 2)
        Some(k -> v)
      } else None
    }.toMap

    Config(
      mode = kv.getOrElse("mode", "teach"),
      shots = kv.get("shots").flatMap(_.toIntOption).getOrElse(2000),
      bins  = kv.get("bins").flatMap(_.toIntOption).getOrElse(61),
      seed  = kv.get("seed").flatMap(_.toLongOption).getOrElse(7L)
    )
  }

  def apply(cfg: Config): Behavior[Command] =
    Behaviors.setup { ctx =>
      val spacetime = ctx.spawn(Spacetime(speed = 1.2, jitterMs = 0.5, seed = cfg.seed), "spacetime")
      val manager   = ctx.spawn(TransactionManager(seed = cfg.seed + 1), "tm")

      cfg.mode match {
        case "teach" =>
          runTeach(ctx, spacetime, manager, cfg)

        case "dslit" =>
          runDoubleSlit(ctx, spacetime, manager, cfg)

        case "bell" =>
          runBell(ctx, spacetime, manager, cfg)

        case other =>
          ctx.log.error(s"Unknown --mode=$other (use teach|dslit|bell)")
          Behaviors.stopped
      }
    }

  private def runTeach(
    ctx: org.apache.pekko.actor.typed.scaladsl.ActorContext[Command],
    spacetime: ActorRef[Spacetime.Command],
    manager: ActorRef[TransactionManager.Command],
    cfg: Config
  ): Behavior[Command] = {

    val emitterPos = Vec2(0.0, 0.0)
    val k = 2.0 * math.Pi / 6.0

    val r1Params = Receiver.Params("R1", pos = Vec2(3.0, 1.0), coupling = Complex(1.00, 0.00), coherenceWindow = 3.millis)
    val r2Params = Receiver.Params("R2", pos = Vec2(6.0, 0.0), coupling = Complex(1.30, 0.00), coherenceWindow = 3.millis)
    val r3Params = Receiver.Params("R3", pos = Vec2(2.0, 5.0), coupling = Complex(0.80, 0.20), coherenceWindow = 3.millis)

    val r1 = ctx.spawn(Receiver(r1Params, spacetime), "R1")
    val r2 = ctx.spawn(Receiver(r2Params, spacetime), "R2")
    val r3 = ctx.spawn(Receiver(r3Params, spacetime), "R3")

    val receivers: Vector[(ActorRef[Receiver.Command], Vec2)] =
      Vector((r1, r1Params.pos), (r2, r2Params.pos), (r3, r3Params.pos))

    def fireEmission(emissionId: Long): Unit = {
      manager ! TransactionManager.BeginSingle(
        emissionId = emissionId,
        receivers = receivers.map(_._1),
        replyTo = ctx.self,
        timeout = 120.millis
      )

      receivers.foreach { case (rref, rpos) =>
        spacetime ! Spacetime.SendOffer(
          emissionId = emissionId,
          sourceId = "S",
          sourcePos = emitterPos,
          k = k,
          offerAmp = Complex.one,
          to = rref,
          toPos = rpos,
          manager = manager
        )
      }
    }

    ctx.self ! Start

    var remaining = cfg.shots
    var nextId: Long = 1L
    var counts: Map[String, Int] = Map.empty

    Behaviors.receiveMessage {
      case Start =>
        fireEmission(nextId)
        Behaviors.same

      case SingleCollapsed(eid, chosen, probs) =>
        val name = chosen.path.name
        counts = counts.updated(name, counts.getOrElse(name, 0) + 1)
        remaining -= 1

        if (eid <= 5) {
          val ps = probs.map { case (r, p) => f"${r.path.name}:$p%.3f" }.mkString(", ")
          ctx.log.info(s"[teach] emission=$eid probs: $ps  chosen=$name")
        }

        if (remaining <= 0) {
          val total = counts.values.sum.toDouble.max(1.0)
          ctx.log.info(s"[teach] done. shots=${cfg.shots}")
          counts.toVector.sortBy(_._1).foreach { case (n, c) =>
            ctx.log.info(f"[teach] $n -> $c (${c / total}%.3f)")
          }
          Behaviors.stopped
        } else {
          nextId += 1
          fireEmission(nextId)
          Behaviors.same
        }

      case SingleNoTransaction(eid) =>
        ctx.log.warn(s"[teach] emission=$eid no transaction")
        remaining -= 1
        if (remaining <= 0) Behaviors.stopped
        else {
          nextId += 1
          fireEmission(nextId)
          Behaviors.same
        }

      case other =>
        ctx.log.debug(s"[teach] ignoring: $other")
        Behaviors.same
    }
  }

  private def runDoubleSlit(
    ctx: org.apache.pekko.actor.typed.scaladsl.ActorContext[Command],
    spacetime: ActorRef[Spacetime.Command],
    manager: ActorRef[TransactionManager.Command],
    cfg: Config
  ): Behavior[Command] = {

    val bins = cfg.bins.max(5)
    val screenY = 12.0
    val xMin = -9.0
    val xMax =  9.0

    val xs = (0 until bins).map(i => xMin + (xMax - xMin) * i.toDouble / (bins - 1)).toVector
    val pixels: Vector[(ActorRef[Receiver.Command], Vec2)] =
      xs.zipWithIndex.map { case (x, idx) =>
        val name = f"P$idx%03d"
        val params = Receiver.Params(
          name = name,
          pos = Vec2(x, screenY),
          coupling = Complex.one,
          coherenceWindow = 3.millis
        )
        (ctx.spawn(Receiver(params, spacetime), name), params.pos)
      }

    val pixelRefs = pixels.map(_._1)

    // Two slits
    val slitY = 0.0
    val slitSep = 2.0
    val slitA = Vec2(-slitSep / 2.0, slitY)
    val slitB = Vec2(+slitSep / 2.0, slitY)

    val k = 2.0 * math.Pi / 2.2

    def fireEmission(emissionId: Long): Unit = {
      manager ! TransactionManager.BeginSingle(
        emissionId = emissionId,
        receivers = pixelRefs,
        replyTo = ctx.self,
        timeout = 180.millis
      )

      // Source A
      pixels.foreach { case (rref, rpos) =>
        spacetime ! Spacetime.SendOffer(
          emissionId = emissionId,
          sourceId = "A",
          sourcePos = slitA,
          k = k,
          offerAmp = Complex.one,
          to = rref,
          toPos = rpos,
          manager = manager
        )
      }

      // Source B
      pixels.foreach { case (rref, rpos) =>
        spacetime ! Spacetime.SendOffer(
          emissionId = emissionId,
          sourceId = "B",
          sourcePos = slitB,
          k = k,
          offerAmp = Complex.one,
          to = rref,
          toPos = rpos,
          manager = manager
        )
      }
    }

    def renderHistogram(counts: Array[Int]): Unit = {
      val max = counts.max.max(1)
      ctx.log.info(s"[dslit] histogram bins=$bins (scaled):")
      counts.zipWithIndex.foreach { case (c, i) =>
        val barN = ((c.toDouble / max) * 60).round.toInt
        val bar = "█" * barN
        ctx.log.info(f"[dslit] $i%03d $bar")
      }
    }

    ctx.self ! Start

    var remaining = cfg.shots
    var nextId: Long = 1L
    var counts = Array.fill(bins)(0)

    Behaviors.receiveMessage {
      case Start =>
        fireEmission(nextId)
        Behaviors.same

      case SingleCollapsed(eid, chosen, _) =>
        val name = chosen.path.name
        if (name.startsWith("P")) {
          val idx = name.drop(1).toIntOption.getOrElse(-1)
          if (idx >= 0 && idx < bins) counts(idx) += 1
        }
        remaining -= 1

        if (remaining <= 0) {
          ctx.log.info(s"[dslit] done. shots=${cfg.shots}")
          renderHistogram(counts)
          Behaviors.stopped
        } else {
          nextId += 1
          fireEmission(nextId)
          Behaviors.same
        }

      case SingleNoTransaction(eid) =>
        ctx.log.warn(s"[dslit] emission=$eid no transaction")
        remaining -= 1
        if (remaining <= 0) {
          renderHistogram(counts)
          Behaviors.stopped
        } else {
          nextId += 1
          fireEmission(nextId)
          Behaviors.same
        }

      case other =>
        ctx.log.debug(s"[dslit] ignoring: $other")
        Behaviors.same
    }
  }

  private def runBell(
    ctx: org.apache.pekko.actor.typed.scaladsl.ActorContext[Command],
    spacetime: ActorRef[Spacetime.Command],
    manager: ActorRef[TransactionManager.Command],
    cfg: Config
  ): Behavior[Command] = {

    val leftPlus  = ctx.spawn(BellDetector(BellDetector.Params("L+", Side.Left, Outcome.Plus), spacetime), "Lplus")
    val leftMinus = ctx.spawn(BellDetector(BellDetector.Params("L-", Side.Left, Outcome.Minus), spacetime), "Lminus")
    val rightPlus  = ctx.spawn(BellDetector(BellDetector.Params("R+", Side.Right, Outcome.Plus), spacetime), "Rplus")
    val rightMinus = ctx.spawn(BellDetector(BellDetector.Params("R-", Side.Right, Outcome.Minus), spacetime), "Rminus")

    val left = Vector(leftPlus, leftMinus)
    val right = Vector(rightPlus, rightMinus)

    val A0 = 0.0
    val A1 = math.Pi / 4.0
    val B0 = math.Pi / 8.0
    val B1 = -math.Pi / 8.0
    val settingPairs = Vector(
      ("A0","B0", A0, B0),
      ("A0","B1", A0, B1),
      ("A1","B0", A1, B0),
      ("A1","B1", A1, B1)
    )

    def idx(a: Outcome, b: Outcome): Int = (a, b) match {
      case (Outcome.Plus, Outcome.Plus)   => 0
      case (Outcome.Plus, Outcome.Minus)  => 1
      case (Outcome.Minus, Outcome.Plus)  => 2
      case (Outcome.Minus, Outcome.Minus) => 3
    }

    def E(arr: Array[Int]): Double = {
      val npp = arr(0).toDouble
      val npm = arr(1).toDouble
      val nmp = arr(2).toDouble
      val nmm = arr(3).toDouble
      val tot = npp + npm + nmp + nmm
      if (tot == 0) 0.0 else (npp - npm - nmp + nmm) / tot
    }

    def report(counts: Map[(String, String), Array[Int]]): Unit = {
      val eA0B0 = E(counts(("A0","B0")))
      val eA0B1 = E(counts(("A0","B1")))
      val eA1B0 = E(counts(("A1","B0")))
      val eA1B1 = E(counts(("A1","B1")))
      val S = eA0B0 + eA0B1 + eA1B0 - eA1B1
      ctx.log.info(f"[bell] E(A0,B0)=$eA0B0%.4f, E(A0,B1)=$eA0B1%.4f, E(A1,B0)=$eA1B0%.4f, E(A1,B1)=$eA1B1%.4f")
      ctx.log.info(f"[bell] CHSH S = $S%.4f (quantum ideal for singlet with these angles is ~2.828)")
    }

    def armAndBegin(emissionId: Long, thetaL: Double, thetaR: Double): Unit = {
      leftPlus  ! BellDetector.Arm(emissionId, thetaL, manager)
      leftMinus ! BellDetector.Arm(emissionId, thetaL, manager)
      rightPlus  ! BellDetector.Arm(emissionId, thetaR, manager)
      rightMinus ! BellDetector.Arm(emissionId, thetaR, manager)

      manager ! TransactionManager.BeginBell(
        emissionId = emissionId,
        thetaL = thetaL,
        thetaR = thetaR,
        left = left,
        right = right,
        replyTo = ctx.self,
        timeout = 60.millis
      )
    }

    ctx.self ! Start

    val shotsPerSetting = cfg.shots.max(1)
    var blockIdx = 0
    var shotIdx = 0
    var emissionId: Long = 1L

    var counts: Map[(String, String), Array[Int]] =
      Map.empty.withDefault(_ => Array.fill(4)(0))

    def currentSetting: (String, String, Double, Double) = settingPairs(blockIdx)

    def startNext(): Unit = {
      if (blockIdx >= settingPairs.size) return
      val (_, _, thetaL, thetaR) = currentSetting
      armAndBegin(emissionId, thetaL, thetaR)
    }

    Behaviors.receiveMessage {
      case Start =>
        startNext()
        Behaviors.same

      case BellCollapsed(eid, _, _, a, b) =>
        val (an, bn, _, _) = currentSetting
        val key = (an, bn)
        val arr = counts(key)
        arr(idx(a, b)) += 1
        counts = counts.updated(key, arr)

        // advance
        shotIdx += 1
        emissionId += 1

        if (shotIdx >= shotsPerSetting) {
          val a2 = counts(key)
          ctx.log.info(s"[bell] block ($an,$bn) done: ++=${a2(0)} +-=${a2(1)} -+=${a2(2)} --=${a2(3)}")
          blockIdx += 1
          shotIdx = 0
        }

        if (blockIdx >= settingPairs.size) {
          ctx.log.info(s"[bell] done. shots/setting=$shotsPerSetting")
          report(counts)
          Behaviors.stopped
        } else {
          startNext()
          Behaviors.same
        }

      case BellNoTransaction(eid) =>
        ctx.log.warn(s"[bell] emission=$eid no transaction")
        // still advance the attempt counter
        shotIdx += 1
        emissionId += 1

        if (shotIdx >= shotsPerSetting) {
          val (an, bn, _, _) = currentSetting
          val a2 = counts((an, bn))
          ctx.log.info(s"[bell] block ($an,$bn) done: ++=${a2(0)} +-=${a2(1)} -+=${a2(2)} --=${a2(3)}")
          blockIdx += 1
          shotIdx = 0
        }

        if (blockIdx >= settingPairs.size) {
          ctx.log.info(s"[bell] done. shots/setting=$shotsPerSetting")
          report(counts)
          Behaviors.stopped
        } else {
          startNext()
          Behaviors.same
        }

      case other =>
        ctx.log.debug(s"[bell] ignoring: $other")
        Behaviors.same
    }
  }
}

object TransactionalAdvanced {
  def main(args: Array[String]): Unit = {
    val cfg = SimulationController.parseArgs(args)
    val system: ActorSystem[SimulationController.Command] =
      ActorSystem(SimulationController(cfg), "ti-unified")

    Await.result(system.whenTerminated, Duration.Inf)
  }
}
