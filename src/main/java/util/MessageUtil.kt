package util

import node.Message

import java.io.*
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.logging.Logger

object MessageUtil {

  private val logger = Logger.getLogger(MessageUtil::class.java.name)

  @Throws(IOException::class)
  fun getMessageBytes(socket: Socket): ByteArray {
    val size = ByteArray(4)
    val clientInputStream = BufferedInputStream(socket.getInputStream())
    var bytesRead = clientInputStream.read(size, 0, 4)
    val wrapped = ByteBuffer.wrap(size) // big-endian by default
    val messageSize = wrapped.int
    var offset = 0
    val data = ByteArray(messageSize)
    bytesRead = clientInputStream.read(data, offset, data.size - offset)
    while (bytesRead != -1) {
      offset += bytesRead
      if (offset >= data.size) {
        break
      }
      bytesRead = clientInputStream.read(data, offset, data.size - offset)
    }
    return data
  }

  @Throws(IOException::class)
  fun sendMessage(socket: Socket, message: Message) {
    val outputStream = BufferedOutputStream(socket.getOutputStream())
    val responseMessageSize = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(message.toByteArray().size).array()
    outputStream.write(responseMessageSize)
    outputStream.write(message.toByteArray())
    outputStream.flush()
  }

}
