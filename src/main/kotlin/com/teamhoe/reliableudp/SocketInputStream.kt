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
class SocketInputStream(val serverSocket:ServerSocket,val remoteAddress:SocketAddress,rttTimeoutMultiplier:Long,initialSequenceNumber:Int,maxWindowSize:Int):InputStream()
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
        fun receive(packet:ISeqPacket)
        fun available():Int
        fun read(b:ByteArray,off:Int,len:Int):Int
        fun close()
    }

    private class EstablishedState(val context:SocketInputStream,val recvWnd:ReceiveWindow,val rttTimeoutMultiplier:Long):State
    {
        private var estimatedRtt:Double = 0.0

        private var currentPayload = ByteBuffer.allocate(0)

        override fun receive(packet:ISeqPacket)
        {
            recvWnd.offer(packet)
            context.serverSocket.udpSocket.send(AckPacket(packet,positiveOffset(packet.sequenceNumber,recvWnd.lastSequenceNumber)).datagram)
            // todo: only take the latest rtt, not every single one
            estimatedRtt = packet.estimatedRtt
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
                    context.state = EofState(context,nextPacket,rttTimeoutMultiplier)
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
            var nextPacket:ISeqPacket? = null
            val takeThread = thread()
            {
                nextPacket = recvWnd.take()
            }
            takeThread.join(estimatedRtt.toLong())
            takeThread.interrupt()

            if (nextPacket is FinPacket)
            {
                val eofState = EofState(context,nextPacket!!,rttTimeoutMultiplier)
                context.state = eofState
                eofState.awaitClosed()
            }
            else
            {
                throw IOException("output stream wasn't closed")
            }
        }
    }

    private class ClosedState(val context:SocketInputStream):State
    {
        init
        {
            context.serverSocket.seqReceivers.remove(context.remoteAddress)
        }
        override fun receive(packet:ISeqPacket) = throw IllegalStateException("stream is closed")
        override fun available():Int = 0
        override fun read(b:ByteArray,off:Int,len:Int):Int = throw IOException("stream is closed")
        override fun close() {}
    }

    private class EofState(val context:SocketInputStream,finPacket:ISeqPacket,val rttTimeoutMultiplier:Long):State
    {
        var delayedCloseThread = Thread()
        val signaledOnClose = CountDownLatch(1)
        init
        {
            receive(finPacket)
        }
        override fun receive(packet:ISeqPacket)
        {
            assert(packet is FinPacket,{"output stream wasn't closed"})
            delayedCloseThread.interrupt()
            delayedCloseThread = thread(name = "delayed close thread")
            {
                try
                {
                    Thread.sleep(packet.estimatedRtt.toLong()*rttTimeoutMultiplier)
                    context.serverSocket.seqReceivers.remove(context.remoteAddress)
                    context.state = ClosedState(context)
                    signaledOnClose.countDown()
                }
                catch (ex:InterruptedException)
                {
                    // do nothing
                }
            }
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
