package node

import com.google.protobuf.ByteString
import handlers.*
import util.MessageUtil
import java.io.BufferedReader
import java.io.FileReader
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantLock
import java.util.logging.Level
import java.util.logging.Logger

class TorrentNode @Throws(Exception::class)
constructor(private val localNode: NodeConfiguration, private val otherNodes: List<NodeConfiguration>) {

  private val server: ServerSocket = ServerSocket(localNode.port, 100, InetAddress.getByName(localNode.addr))
  private val localFiles: MutableMap<ByteString, MutableList<ByteArray>>
  private val fileNameToHash: MutableMap<String, ByteString>

  private val executor = Executors.newFixedThreadPool(10)
  private val lock = ReentrantLock()

  val socketAddress: InetAddress
    get() = this.server.inetAddress

  val port: Int
    get() = this.server.localPort

  init {
    this.localFiles = HashMap()
    this.fileNameToHash = HashMap()
  }

  @Throws(Exception::class)
  private fun listen() {
    while (true) {
      logger.info("Waiting for connection")
      val clientSocket = this.server.accept()
      executor.submit { handleClient(clientSocket) }
    }
  }

  private fun handleClient(clientSocket: Socket) {
    val clientAddress = clientSocket.inetAddress.hostAddress
    logger.info("New connection from " + clientAddress)
    try {
      val buffer = MessageUtil.getMessageBytes(clientSocket)
      if (buffer != null) {
        val message = Message.parseFrom(buffer)
        var responseMessage: Message? = null
        logger.fine(clientSocket.toString() + " " + message.toString())
        if (message.type == Message.Type.LOCAL_SEARCH_REQUEST) {
          logger.info(clientSocket.toString() + " Local search request")
          lock.lock()
          responseMessage = LocalSearchRequestHandler.handleLocalSearchRequest(message, localFiles, fileNameToHash)
          lock.unlock()
        } else if (message.type == Message.Type.SEARCH_REQUEST) {
          logger.info(clientSocket.toString() + " Search request")
          lock.lock()
          responseMessage = SearchRequestHandler.handleSearchRequest(message, localNode, otherNodes, localFiles, fileNameToHash)
          lock.unlock()
        } else if (message.type == Message.Type.UPLOAD_REQUEST) {
          logger.info(clientSocket.toString() + " Upload request")
          lock.lock()
          responseMessage = UploadRequestHandler.handleUploadRequest(message, localFiles, fileNameToHash)
          lock.unlock()
        } else if (message.type == Message.Type.REPLICATE_REQUEST) {
          logger.info(clientSocket.toString() + " Replicate request")
          lock.lock()
          responseMessage = ReplicateRequestHandler.handleReplicateRequest(message, otherNodes, localFiles, fileNameToHash)
          lock.unlock()
        } else if (message.type == Message.Type.CHUNK_REQUEST) {
          logger.info(clientSocket.toString() + " Chunk request")
          lock.lock()
          responseMessage = ChunkRequestHandler.handleChunkRequest(message, localFiles)
          lock.unlock()
        } else if (message.type == Message.Type.DOWNLOAD_REQUEST) {
          logger.info(clientSocket.toString() + " Download request")
          lock.lock()
          responseMessage = DownloadRequestHandler.handleDownloadRequest(message, localFiles)
          lock.unlock()
        }
        if (responseMessage != null) {
          MessageUtil.sendMessage(clientSocket, responseMessage)
        }
      }
    } catch (e: IOException) {
      logger.log(Level.SEVERE, e.message)
    } finally {
      try {
        clientSocket.close()
      } catch (e: IOException) {
        logger.log(Level.SEVERE, e.message)
      }

    }
  }

  companion object {

    private val logger = Logger.getLogger(TorrentNode::class.java.name)

    private const val confFile = "torrent.conf"
    private const val ipPrefixKey = "ip-prefix"
    private const val portBaseKey = "port-base"
    private const val ipSuffixesKey = "ip-suffixes"
    private const val portOffsetsKey = "port-offsets"

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
      var ipPrefix = ""
      var portBase = 0
      var ipSuffixes = arrayOfNulls<String>(0)
      var portOffsets = arrayOfNulls<String>(0)

      try {
        BufferedReader(FileReader(confFile)).use { br ->
          for (i in 0..3) {
            val sCurrentLine = br.readLine()
            val lineElements = sCurrentLine.split("=".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
            val key = lineElements[0]
            val value = lineElements[1]
            when (key) {
              ipPrefixKey -> ipPrefix = value
              portBaseKey -> portBase = Integer.parseInt(value)
              ipSuffixesKey -> ipSuffixes = value.split(" ".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
              portOffsetsKey -> portOffsets = value.split(" ".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
            }
          }
        }
      } catch (e: IOException) {
        logger.log(Level.SEVERE, e.message)
      }

      val otherNodes = ArrayList<NodeConfiguration>()
      for (ipSuffix in ipSuffixes) {
        portOffsets.mapTo(otherNodes) { getNodeConfiguration(ipPrefix, portBase, ipSuffix!!, it!!) }
      }

      val ipPort = args[0]
      val ipAndPort = ipPort.split(":".toRegex()).dropLastWhile({ it.isEmpty() }).toTypedArray()
      val currentConfiguration = getNodeConfiguration(ipPrefix, portBase, ipAndPort[0], ipAndPort[1])
      if (!otherNodes.contains(currentConfiguration)) {
        logger.log(Level.SEVERE, "Invalid node configuration")
      } else {
        otherNodes.remove(currentConfiguration)
        val app = TorrentNode(currentConfiguration, otherNodes)

        logger.info("Running Server: " +
                "Host=" + app.socketAddress.hostAddress +
                " Port=" + app.port)

        app.listen()
      }
    }

    private fun getNodeConfiguration(ipPrefix: String, portBase: Int, ipSuffix: String, portOffset: String): NodeConfiguration {
      val bindAddr = ipPrefix + "." + ipSuffix
      val port = portBase + Integer.parseInt(portOffset)
      return NodeConfiguration(bindAddr, port)
    }
  }
}
