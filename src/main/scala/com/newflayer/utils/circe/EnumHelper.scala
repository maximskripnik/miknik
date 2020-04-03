package com.newflayer.utils.circe

import io.circe.Codec
import io.circe.Decoder
import io.circe.Encoder

object EnumHelper {

  def buildEncoder[A](f: A => String): Encoder[A] = Encoder.encodeString.contramap(f)

  def buildDecoder[A](f: PartialFunction[String, A]): Decoder[A] =
    Decoder.decodeString.emap { str => f.lift(str).toRight(s"Unknown value '$str'") }

  def buildCodec[A](f: A => String, g: PartialFunction[String, A]): Codec[A] =
    Codec.from(buildDecoder(g), buildEncoder(f))

}
