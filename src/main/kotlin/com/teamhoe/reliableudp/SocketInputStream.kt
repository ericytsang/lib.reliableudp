package com.teamhoe.reliableudp

import com.teamhoe.reliableudp.net.AckPacket
import com.teamhoe.reliableudp.net.DataPacket
import com.teamhoe.reliableudp.net.FinPacket
import com.teamhoe.reliableudp.net.ISeqPacket
import com.teamhoe.reliableudp.net.ReceiveWindow
import com.teamhoe.reliableudp.utils.positiveOffset
import java.io.IOException
import java.io.InputStream
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.thread

/**
 * Created by Eric on 4/8/2016.
 */
class SocketInputStream(val serverSocket:ServerSocket,val remoteAddress:SocketAddress,rttTimeoutMultiplier:Double,initialSequenceNumber:Int,maxWindowSize:Int):InputStream()
{
    private var state:State = EstablishedState(this,ReceiveWindow(initialSequenceNumber,maxWindowSize),rttTimeoutMultiplier)
    internal fun receive(packet:ISeqPacket) = state.receive(packet)
    override fun available():Int = state.available()
    override fun read():Int
    {
        val b = ByteArray(1)
        while (true)
        {
            when (read(b))
            {
                -1 -> return -1
                0 -> {/*continue*/}
                1 -> return (b[0].toInt() and 0xFF)
                else -> throw RuntimeException("wtf")
            }
        }
    }
    override fun read(b:ByteArray):Int = read(b,0,b.size)
    override fun read(b:ByteArray,off:Int,len:Int):Int = state.read(b,off,len)
    override fun close() = state.close()

    private interface State
    {
        fun receive(packet:ISeqPacket):Boolean
        fun available():Int
        fun read(b:ByteArray,off:Int,len:Int):Int
        fun close()
    }

    private class EstablishedState(val context:SocketInputStream,val recvWnd:ReceiveWindow,val rttTimeoutMultiplier:Double):State
    {
        private var estimatedRtt:Double = 0.0

        private var currentPayload = ByteBuffer.allocate(0)

        override fun receive(packet:ISeqPacket):Boolean
        {
            val result = recvWnd.offer(packet)
            context.serverSocket.udpSocket.send(AckPacket(packet,positiveOffset(packet.sequenceNumber,recvWnd.lastSequenceNumber)).datagram)
            // todo: only take the latest rtt, not every single one
            estimatedRtt = packet.estimatedRtt
            return result
        }

        override fun available():Int = currentPayload.remaining()

        override fun read(b:ByteArray,off:Int,len:Int):Int
        {
            // assign current payload to the next data packet if it is empty
            while (!currentPayload.hasRemaining())
            {
                val nextPacket = recvWnd.take()
                if (nextPacket is DataPacket)
                {
                    currentPayload = ByteBuffer.wrap(nextPacket.payload)
                }
                else if (nextPacket is FinPacket)
                {
                    context.state = EofState(context,nextPacket,rttTimeoutMultiplier,recvWnd.lastSequenceNumber)
                    return context.state.read(b,off,len)
                }
            }

            // read from the current payload into the user's data buffer
            val initialRemaining = currentPayload.remaining()
            currentPayload.get(b,off,Math.min(len,currentPayload.remaining()))
            return initialRemaining-currentPayload.remaining()
        }

        override fun close()
        {
            context.serverSocket.secondarySeqReceivers.put(context.remoteAddress,SocketInputStreamSeqReceiverAdapter(context))
            context.serverSocket.primarySeqReceivers.remove(context.remoteAddress)

            val nextPacket = recvWnd.take()
            if (nextPacket is FinPacket)
            {
                val eofState = EofState(context,nextPacket,rttTimeoutMultiplier,recvWnd.lastSequenceNumber)
                context.state = eofState
                eofState.awaitClosed()
            }
            else
            {
                throw IOException("remote output stream wasn't closed: $nextPacket")
            }
        }
    }

    private class ClosedState(val context:SocketInputStream):State
    {
        init
        {
            context.serverSocket.secondarySeqReceivers.remove(context.remoteAddress)
        }
        override fun receive(packet:ISeqPacket) = false
        override fun available():Int = 0
        override fun read(b:ByteArray,off:Int,len:Int):Int = throw IOException("stream is closed")
        override fun close() {}
    }

    private class EofState(val context:SocketInputStream,finPacket:ISeqPacket,val rttTimeoutMultiplier:Double,val lastSequenceNumber:Int):State
    {
        var delayedCloseThread = Thread()
        val signaledOnClose = CountDownLatch(1)
        init
        {
            receive(finPacket)
        }
        override fun receive(packet:ISeqPacket):Boolean
        {
            context.serverSocket.udpSocket.send(AckPacket(packet,positiveOffset(packet.sequenceNumber,lastSequenceNumber)).datagram)
            delayedCloseThread.interrupt()
            delayedCloseThread = thread(name = "delayed close thread")
            {
                try
                {
                    Thread.sleep((packet.estimatedRtt*rttTimeoutMultiplier).toLong())
                    context.state = ClosedState(context)
                    signaledOnClose.countDown()
                }
                catch (ex:InterruptedException)
                {
                    // do nothing
                }
            }
            return true
        }
        fun awaitClosed()
        {
            signaledOnClose.await()
        }
        override fun available():Int = 0
        override fun read(b:ByteArray,off:Int,len:Int):Int = -1
        override fun close() { }
    }
}
