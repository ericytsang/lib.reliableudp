package com.teamhoe.reliableudp.net

import com.teamhoe.reliableudp.utils.UnsignedInt
import java.net.DatagramPacket
import java.net.SocketAddress
import java.nio.ByteBuffer

internal interface Packet
{
    companion object
    {
        val SIZE_OF_SHORT:Int = 2
        val SIZE_OF_INT:Int = 4
        val DATA_PACKET_HEADER_LEN:Int = 8
        val MAX_PROTOCOL_DATAGRAM_PAYLOAD_LEN:Int = 1400

        fun parse(datagramPacket:DatagramPacket):Packet
        {
            val datagramPayload = ByteBuffer.wrap(datagramPacket.data)
            val type = Packet.Type.values()[datagramPayload.short.toInt()]

            when(type)
            {
                Packet.Type.SYN ->
                {
                    val sequenceNumber = datagramPayload.int
                    return SynPacket(datagramPacket.socketAddress,sequenceNumber)
                }
                Packet.Type.DATA ->
                {
                    val sequenceNumber = datagramPayload.int
                    val payload = ByteArray(datagramPacket.length-Packet.Companion.SIZE_OF_SHORT-Packet.Companion.SIZE_OF_INT)
                    datagramPayload.get(payload)
                    return DataPacket(datagramPacket.socketAddress,sequenceNumber,payload)
                }
                Packet.Type.ACK ->
                {
                    val acknowledgmentNumber = datagramPayload.int
                    val window = UnsignedInt.fromInt(datagramPayload.int).value
                    return AckPacket(datagramPacket.socketAddress,acknowledgmentNumber,window)
                }
                Packet.Type.FIN ->
                {
                    val sequenceNumber = datagramPayload.int
                    return FinPacket(datagramPacket.socketAddress,sequenceNumber)
                }
                else -> throw IllegalArgumentException("unknown packet type!")
            }
        }
    }

    enum class Type { SYN, DATA, ACK, FIN }

    val remoteAddress:SocketAddress
    val type:Packet.Type
    val datagram:DatagramPacket
}

internal interface ISeqPacket:Packet
{
    val sequenceNumber:Int
        get
}

internal interface IAckPacket:Packet
{
    val acknowledgementNumber:Int
    val window:UnsignedInt
}

internal data class SynPacket(
    override val remoteAddress:SocketAddress,
    override val sequenceNumber:Int = (Math.random()*Int.MAX_VALUE).toInt()):
    ISeqPacket
{
    override val type:Packet.Type
        get() = Packet.Type.SYN
    override val datagram:DatagramPacket
        get()
        {
            val datagramPayload = ByteBuffer
                .allocate(Packet.Companion.SIZE_OF_SHORT+Packet.Companion.SIZE_OF_INT)
                .putShort(type.ordinal.toShort())
                .putInt(sequenceNumber)
                .array()
            return DatagramPacket(datagramPayload,datagramPayload.size,remoteAddress)
        }
}

internal data class DataPacket(
    override val remoteAddress:SocketAddress,
    override val sequenceNumber:Int,
    val payload:ByteArray):
    ISeqPacket
{
    init
    {
        if(payload.size > Packet.Companion.MAX_PROTOCOL_DATAGRAM_PAYLOAD_LEN)
            throw IllegalArgumentException("did not meet precondition: \"_payload.size(${payload.size}) > Packet.MAX_PROTOCOL_DATAGRAM_PAYLOAD_LEN(${Packet.Companion.MAX_PROTOCOL_DATAGRAM_PAYLOAD_LEN})\".")
    }

    override val type:Packet.Type
        get() = Packet.Type.DATA
    override val datagram:DatagramPacket
        get()
        {
            val datagramPayload = ByteBuffer
                .allocate(Packet.Companion.SIZE_OF_SHORT+Packet.Companion.SIZE_OF_INT+payload.size)
                .putShort(type.ordinal.toShort())
                .putInt(sequenceNumber)
                .put(payload)
                .array()
            return DatagramPacket(datagramPayload,datagramPayload.size,remoteAddress)
        }
}

internal data class AckPacket(
    override val remoteAddress:SocketAddress,
    override val acknowledgementNumber:Int,
    private val _window:Long):
    IAckPacket
{
    constructor(packetToAck:ISeqPacket,window:Long):this(
        remoteAddress = packetToAck.remoteAddress,
        acknowledgementNumber = packetToAck.sequenceNumber,
        _window = window)

    override val type:Packet.Type
        get() = Packet.Type.ACK
    override val window:UnsignedInt
        get() = UnsignedInt.fromLong(_window)
    override val datagram:DatagramPacket
        get()
        {
            val datagramPayload = ByteBuffer
                .allocate(Packet.Companion.SIZE_OF_SHORT+Packet.Companion.SIZE_OF_INT*2)
                .putShort(type.ordinal.toShort())
                .putInt(acknowledgementNumber)
                .putInt(window.bytes)
                .array()
            return DatagramPacket(datagramPayload,datagramPayload.size,remoteAddress)
        }
}

internal data class FinPacket(
    override val remoteAddress:SocketAddress,
    override val sequenceNumber:Int):
    ISeqPacket
{
    override val type:Packet.Type
        get() = Packet.Type.FIN
    override val datagram:DatagramPacket
        get()
        {
            val datagramPayload = ByteBuffer
                .allocate(Packet.Companion.SIZE_OF_SHORT+Packet.Companion.SIZE_OF_INT)
                .putShort(type.ordinal.toShort())
                .putInt(sequenceNumber)
                .array()
            return DatagramPacket(datagramPayload,datagramPayload.size,remoteAddress)
        }
}
