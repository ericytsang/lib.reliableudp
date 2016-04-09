package com.teamhoe.reliableudp

import org.junit.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
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
        val ss = ServerSocket.make()
        val port = ss.localPort
        ss.close()
        val ss2 = ServerSocket.make(port)
        assert(ss2.localPort == port)
        ss2.close()
    }

    @Test
    fun simplexTraffic()
    {
        val rendezvous = CyclicBarrier(2)
        val svrSocket1 = ServerSocket.make(7000)
        val svrSocket2 = ServerSocket.make(7001)
        val t1 = thread(name = "t1")
        {
            rendezvous.await()

            val ins = DataInputStream(svrSocket1.accept())
            repeat(5000)
            {
                println(">>>: ${ins.readUTF()}")
            }
            ins.close()
            System.err.println("closed input")
        }
        val t2 = thread(name = "t2")
        {
            rendezvous.await()
            Thread.sleep(10)

            val outs = DataOutputStream(svrSocket2.connect(InetSocketAddress("localhost",7005)))
            for (i in 1..5000)
            {
                outs.writeUTF("transmit $i")
            }
            outs.close()
            System.err.println("closed output")
        }
        t1.join()
        t2.join()
        svrSocket1.close()
        svrSocket2.close()
    }

    @Test
    fun simplexTraffic2()
    {
        val rendezvous = CyclicBarrier(2)
        val svrSocket1 = ServerSocket.make(7000)
        val svrSocket2 = ServerSocket.make(7001)
        val t1 = thread(name = "t1")
        {
            rendezvous.await()

            val ins = DataInputStream(svrSocket1.accept())
            repeat(5000)
            {
                println(">>>: ${ins.readUTF()}")
            }
            ins.close()
            System.err.println("closed input")
        }
        val t2 = thread(name = "t2")
        {
            rendezvous.await()
            Thread.sleep(10)

            val outs = DataOutputStream(svrSocket2.connect(InetSocketAddress("localhost",7005)))
            for (i in 1..5000)
            {
                outs.writeUTF("transmit $i")
            }
            outs.close()
            System.err.println("closed output")
        }
        t1.join()
        t2.join()
        svrSocket1.close()
        svrSocket2.close()
    }

    @Test
    fun readingUntilEof()
    {
        val rendezvous = CyclicBarrier(2)
        val t1 = thread(name = "t1")
        {
            val svrSocket = ServerSocket.make(7006)
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
            val svrSocket = ServerSocket.make()
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
