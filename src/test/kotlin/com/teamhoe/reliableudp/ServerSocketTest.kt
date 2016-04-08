package com.teamhoe.reliableudp

import org.junit.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.InetSocketAddress
import java.util.concurrent.CyclicBarrier
import kotlin.concurrent.thread

/**
 * Created by Eric Tsang on 4/8/2016.
 */
class ServerSocketTest
{
    @Test
    fun serverSocketOpensOnPort()
    {
        val ss = ServerSocket()
        val port = ss.localPort
        ss.close()
        val ss2 = ServerSocket(port)
        assert(ss2.localPort == port)
        ss2.close()
    }

    @Test
    fun simplexTraffic()
    {
        val rendezvous = CyclicBarrier(2)
        val t1 = thread(name = "t1")
        {
            val svrSocket = ServerSocket(7006)
            rendezvous.await()

            val ins = DataInputStream(svrSocket.accept())
            repeat(5)
            {
                println(">>>: ${ins.readUTF()}")
            }
            svrSocket.close()
        }
        val t2 = thread(name = "t2")
        {
            val svrSocket = ServerSocket()
            rendezvous.await()
            Thread.sleep(10)

            val outs = DataOutputStream(svrSocket.connect(InetSocketAddress("localhost",7006)))
            for (i in 1..5)
            {
                outs.writeUTF("transmit $i")
            }
            outs.flush()
            svrSocket.close()
        }
        t1.join()
        t2.join()
    }

    @Test
    fun readingUntilEof()
    {
        val rendezvous = CyclicBarrier(2)
        val t1 = thread(name = "t1")
        {
            val svrSocket = ServerSocket(7006)
            rendezvous.await()

            val ins = svrSocket.accept()
            assert(ins.read() == 1)
            assert(ins.read() == 1)
            assert(ins.read() == 1)
            assert(ins.read() == -1)
            svrSocket.close()
        }
        val t2 = thread(name = "t2")
        {
            val svrSocket = ServerSocket()
            rendezvous.await()
            Thread.sleep(10)

            val outs = svrSocket.connect(InetSocketAddress("localhost",7006))
            for (i in 1..3)
            {
                outs.write(1)
            }
            outs.close()
            svrSocket.close()
        }
        t1.join()
        t2.join()
    }
}
