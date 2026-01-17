package com.sinanspd.qpi

//can instead import these from qure to prevent copy paste 
final case class Complex(re: Double, im: Double) {
  def +(o: Complex): Complex = Complex(re + o.re, im + o.im)
  def *(o: Complex): Complex = Complex(re * o.re - im * o.im, re * o.im + im * o.re)
  def *(k: Double): Complex  = Complex(re * k, im * k)
  def conj: Complex          = Complex(re, -im)
  def abs2: Double           = re * re + im * im
  override def toString: String = f"$re%.4f${if (im < 0) "" else "+"}${im}%.4fi"
}

object Complex {
  val zero: Complex = Complex(0.0, 0.0)
  val one: Complex  = Complex(1.0, 0.0)
}

final case class Vec2(x: Double, y: Double) {
  def dist(o: Vec2): Double = math.hypot(x - o.x, y - o.y)
}
