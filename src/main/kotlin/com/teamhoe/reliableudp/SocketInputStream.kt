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

/**
 * Created by Eric on 4/8/2016.
 */
class SocketInputStream(val serverSocket:ServerSocket,val remoteAddress:SocketAddress,initialSequenceNumber:Int,maxWindowSize:Int):InputStream()
{
    private var state:State = EstablishedState(this,ReceiveWindow(initialSequenceNumber,maxWindowSize))
    internal fun receive(packet:ISeqPacket) = state.receive(packet)
    override fun available():Int = state.available()
    override fun read():Int
    {
        val b = ByteArray(1)
        if (read(b) == -1)
        {
            return -1
        }
        else
        {
            return b[0].toInt()
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

    private class EstablishedState(val context:SocketInputStream,val recvWnd:ReceiveWindow):State
    {
        private var currentPayload = ByteBuffer.allocate(0)

        override fun receive(packet:ISeqPacket)
        {
            recvWnd.offer(packet)
            context.serverSocket.udpSocket.send(AckPacket(packet,positiveOffset(packet.sequenceNumber,recvWnd.lastSequenceNumber)).datagram)
        }

        override fun available():Int = currentPayload.remaining()

        override fun read(b:ByteArray,off:Int,len:Int):Int
        {
            prepareStream()
            if (available() > 0)
            {
                val initialRemaining = currentPayload.remaining()
                currentPayload.get(b,off,len)
                return initialRemaining-currentPayload.remaining()
            }
            else
            {
                context.state = EofState(context)
                return context.state.read(b,off,len)
            }
        }

        override fun close()
        {
            context.state = ClosedState(context)
        }

        /**
         * if [currentPayload] is empty, blocks until the a [DataPacket]
         * arrives, and sets [currentPayload] to reference it, returning
         * true. if the next packet was a [FinPacket], then returns false.
         */
        fun prepareStream() = synchronized(this)
        {
            while (!currentPayload.hasRemaining())
            {
                val nextPacket = recvWnd.take()
                if (nextPacket is DataPacket)
                {
                    currentPayload = ByteBuffer.wrap(nextPacket.payload)
                }
                else if (nextPacket is FinPacket)
                {
                    context.state = ClosedState(context)
                    break
                }
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

    private class EofState(val context:SocketInputStream):State
    {
        init
        {
            context.serverSocket.seqReceivers.remove(context.remoteAddress)
        }
        override fun receive(packet:ISeqPacket) = throw IllegalStateException("stream is eof")
        override fun available():Int = 0
        override fun read(b:ByteArray,off:Int,len:Int):Int = -1
        override fun close() {context.state = ClosedState(context)}
    }
}
