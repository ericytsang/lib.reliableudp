package com.teamhoe.reliableudp

import com.teamhoe.reliableudp.net.CongestionWindow
import com.teamhoe.reliableudp.net.DataPacket
import com.teamhoe.reliableudp.net.FinPacket
import com.teamhoe.reliableudp.net.IAckPacket
import com.teamhoe.reliableudp.net.Packet
import java.io.IOException
import java.io.OutputStream
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.util.LinkedList
import kotlin.concurrent.thread

/**
 * Created by Eric on 4/7/2016.
 */
class SocketOutputStream(val serverSocket:ServerSocket,val remoteAddress:SocketAddress,initialSequenceNumber:Int,maxAllowedConsecutiveDroppedPackets:Int,estinatedRttErrorMargin:Long,initialEstimatedRtt:Double,initialMaxBytesInFlight:Double):OutputStream()
{
    private var state:State = EstablishedState(this,initialSequenceNumber,CongestionWindow(estinatedRttErrorMargin,initialEstimatedRtt,initialMaxBytesInFlight),maxAllowedConsecutiveDroppedPackets)
    internal fun receive(packet:IAckPacket) = state.receive(packet)
    override fun write(b:Int) = write(byteArrayOf(b.toByte()))
    override fun write(b:ByteArray) = write(b,0,b.size)
    override fun write(b:ByteArray,off:Int,len:Int) = state.write(b,off,len)
    override fun flush() = state.flush()
    override fun close() = state.close()

    private interface State
    {
        fun receive(packet:IAckPacket)
        fun write(b:ByteArray,off:Int,len:Int)
        fun flush()
        fun close()
    }

    private class EstablishedState(val context:SocketOutputStream,initialSequenceNumber:Int,val congWnd:CongestionWindow,val maxAllowedConsecutiveDroppedPackets:Int):State
    {
        private var nextSequenceNumber:Int = initialSequenceNumber
            get()
            {
                // todo: can this be refactored to "return field++"?? test it please
                field += 1
                return field-1
            }

        override fun write(b:ByteArray,off:Int,len:Int)
        {
            val data = ByteBuffer.wrap(b,off,len)
            val maxPayloadLength = Packet.MAX_PROTOCOL_DATAGRAM_PAYLOAD_LEN-Packet.DATA_PACKET_HEADER_LEN
            val packets = LinkedList<DataPacket>()
            while (data.hasRemaining())
            {
                val datagramPayload = ByteArray(Math.min(maxPayloadLength,data.remaining()))
                data.get(datagramPayload)
                packets.add(DataPacket(context.remoteAddress,nextSequenceNumber,datagramPayload))
            }
            congWnd.put(packets)
        }

        override fun flush()
        {
            congWnd.flush()
        }

        override fun close()
        {
            // add fin packet
            congWnd.put(listOf(FinPacket(context.remoteAddress,nextSequenceNumber)))

            // flush all the congestion window
            flush()

            // change the state...
            context.state = ClosedState(context)

            // shutdown threads
            sendThread.interrupt()
        }

        override fun receive(packet:IAckPacket)
        {
            congWnd.ack(packet.acknowledgementNumber,packet.window)
        }

        private val sendThread = thread(isDaemon = true,name = "${this.context}.sendThread")
        {
            try
            {
                while (true)
                {
                    // send the next packet given to us by the congestion window
                    val packet = congWnd.take()
                    context.serverSocket.udpSocket.send(packet.datagram)

                    // switch to the error state if dropped too many packets
                    if (congWnd.consecutiveDroppedPacketCount > maxAllowedConsecutiveDroppedPackets)
                    {
                        context.state = ErrorState(context)
                        break
                    }
                }
            }
            catch(ex:InterruptedException)
            {
                return@thread
            }
        }
    }

    private class ErrorState(val context:SocketOutputStream):State
    {
        init
        {
            context.serverSocket.ackReceivers.remove(context.remoteAddress)
        }
        override fun receive(packet:IAckPacket) = throw IllegalStateException("socket is in error")
        override fun write(b:ByteArray,off:Int,len:Int) = throw IOException("stream was unintentionally disconnected; operation cannot be completed")
        override fun flush() = throw IOException("stream was unintentionally disconnected; operation cannot be completed")
        override fun close() {}
    }

    private class ClosedState(val context:SocketOutputStream):State
    {
        init
        {
            context.serverSocket.ackReceivers.remove(context.remoteAddress)
        }
        override fun receive(packet:IAckPacket) = throw IllegalStateException("socket is closed")
        override fun write(b:ByteArray,off:Int,len:Int) = throw IllegalStateException("socket is closed")
        override fun flush() = throw IllegalStateException("socket is closed")
        override fun close() {}
    }
}
