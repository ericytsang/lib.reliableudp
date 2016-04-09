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
import java.util.*
import kotlin.concurrent.thread

/**
 * Created by Eric on 4/7/2016.
 */
class SocketOutputStream(val serverSocket:ServerSocket,val remoteAddress:SocketAddress,initialSequenceNumber:Int,maxAllowedConsecutiveDroppedPackets:Int,initialEstimatedRttErrorMargin:Long,initialEstimatedRtt:Double,initialMaxBytesInFlight:Double):OutputStream()
{
    private var state:State = EstablishedState(this,initialSequenceNumber,CongestionWindow(initialEstimatedRttErrorMargin,initialEstimatedRtt,initialMaxBytesInFlight),maxAllowedConsecutiveDroppedPackets)
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

    private class EstablishedState(val context:SocketOutputStream,initialSequenceNumber:Int,val congWnd:CongestionWindow,val maxAllowedConsecutiveDroppedPacketsPerPacket:Int):State
    {
        private var nextSequenceNumber:Int = initialSequenceNumber
            get()
            {
                // todo: can this be refactored to "return field++"?? test it please
                field += 1
                return field-1
            }

        private var sendingThreads:MutableSet<Thread> = LinkedHashSet()

        override fun write(b:ByteArray,off:Int,len:Int)
        {
            val data = ByteBuffer.wrap(b,off,len)
            val maxPayloadLength = Packet.MAX_PROTOCOL_DATAGRAM_PAYLOAD_LEN-Packet.DATA_PACKET_HEADER_LEN
            val packets = LinkedList<DataPacket>()
            while (data.hasRemaining())
            {
                val datagramPayload = ByteArray(Math.min(maxPayloadLength,data.remaining()))
                data.get(datagramPayload)
                packets.add(DataPacket(context.remoteAddress,nextSequenceNumber,congWnd.estimatedRtt,datagramPayload))
            }
            try
            {
                sendingThreads.add(Thread.currentThread())
                congWnd.put(packets)
            }
            catch (ex:InterruptedException)
            {
                context.state.write(b,off,len)
            }
            finally
            {
                sendingThreads.remove(Thread.currentThread())
            }
        }

        override fun flush()
        {
            try
            {
                sendingThreads.add(Thread.currentThread())
                congWnd.flush()
            }
            catch (ex:InterruptedException)
            {
                context.state.flush()
            }
            finally
            {
                sendingThreads.remove(Thread.currentThread())
            }
        }

        override fun close()
        {
            // add fin packet
            congWnd.put(listOf(FinPacket(context.remoteAddress,nextSequenceNumber,congWnd.estimatedRtt)))

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

                    // switch to the error state if dropped too many packets
                    if (congWnd.consecutiveDroppedPacketCount > congWnd.numPacketsInFlight*maxAllowedConsecutiveDroppedPacketsPerPacket)
                    {
                        context.state = ErrorState(context)
                        sendingThreads.forEach {it.interrupt()}
                        break
                    }
                    else
                    {
                        context.serverSocket.udpSocket.send(packet.datagram)
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
