package handlers

import com.google.protobuf.ByteString
import node.*
import util.MessageUtil

import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.LinkedList
import java.util.logging.Level
import java.util.logging.Logger

object ReplicateRequestHandler {

  private val logger = Logger.getLogger(ReplicateRequestHandler::class.java.name)

  fun handleReplicateRequest(message: Message, otherNodes: List<NodeConfiguration>, localFiles: MutableMap<ByteString, MutableList<ByteArray>>, fileNameToHash: MutableMap<String, ByteString>): Message {
    val builder = ReplicateResponse.newBuilder()
    val replicateRequest = message.replicateRequest
    try {
      val fileInfo = replicateRequest.fileInfo
      val filename = fileInfo.filename
      val fileHash = fileInfo.hash

      var done = false
      if (filename.isEmpty()) {
        builder.status = Status.MESSAGE_ERROR
        done = true
      } else if (fileNameToHash.containsKey(filename) || localFiles.containsKey(fileHash)) {
        builder.status = Status.SUCCESS
        done = true
      }

      if (!done) {
        val chunkInfoList = fileInfo.chunksList
        var lastValidNodeIndex = 0
        val md = MessageDigest.getInstance("MD5")

        for (chunkInfo in chunkInfoList) {
          var nodeIndex = lastValidNodeIndex
          var otherNode = otherNodes[nodeIndex]
          var socket = Socket(InetAddress.getByName(otherNode.addr), otherNode.port)

          val chunkRequest = ChunkRequest.newBuilder().setFileHash(fileHash).setChunkIndex(chunkInfo.index).build()
          val chunkRequestMessage = Message.newBuilder().setType(Message.Type.CHUNK_REQUEST).setChunkRequest(chunkRequest).build()

          while (nodeIndex < otherNodes.size) {
            try {
              MessageUtil.sendMessage(socket, chunkRequestMessage)
              val buffer = MessageUtil.getMessageBytes(socket)
              if (buffer != null) {
                val chunkResponseMessage = Message.parseFrom(buffer).chunkResponse
                if (chunkResponseMessage.status == Status.SUCCESS) {
                  lastValidNodeIndex = nodeIndex
                  val digest: ByteArray
                  val chunkData = chunkResponseMessage.data.toByteArray()
                  md.update(chunkData)
                  digest = md.digest()
                  if (chunkInfo.hash != ByteString.copyFrom(digest) || chunkData.size != chunkInfo.size) {
                    logger.severe("Invalid chunk hash or chunk data size")
                    throw IllegalArgumentException()
                  }
                  val fileContent = localFiles.getOrPut<ByteString, MutableList<ByteArray>>(fileHash) { LinkedList<ByteArray>() }
                  fileContent.add(chunkData)
                  fileNameToHash[filename] = fileHash
                  logger.fine(chunkResponseMessage.toString())
                } else if (chunkResponseMessage.status == Status.UNABLE_TO_COMPLETE) {
                  logger.fine("Unable to get chunk $chunkInfo from node $otherNode")
                  throw IllegalArgumentException()
                }
                val node = Node.newBuilder().setPort(otherNode.port).setHost(otherNode.addr).build()
                val nodeReplicationStatus = NodeReplicationStatus.newBuilder().setNode(node).setChunkIndex(chunkRequest.chunkIndex).setStatus(Status.SUCCESS).build()
                builder.addNodeStatusList(nodeReplicationStatus)
              }

              logger.fine(fileInfo.toString())
              break
            } catch (e: IllegalArgumentException) {
              nodeIndex++
              socket.close()
              otherNode = otherNodes[nodeIndex]
              socket = Socket(InetAddress.getByName(otherNode.addr), otherNode.port)
            }

          }

          socket.close()

          if (nodeIndex == otherNodes.size) {
            builder.status = Status.UNABLE_TO_COMPLETE
            break
          }
        }
      }
    } catch (e: IOException) {
      builder.status = Status.UNABLE_TO_COMPLETE
      logger.log(Level.SEVERE, e.message, e)
    } catch (e: NoSuchAlgorithmException) {
      logger.log(Level.SEVERE, e.message, e)
    }

    if (builder.nodeStatusListBuilderList.size != replicateRequest.fileInfo.chunksList.size) {
      builder.status = Status.UNABLE_TO_COMPLETE
    }

    return Message.newBuilder().setType(Message.Type.REPLICATE_RESPONSE).setReplicateResponse(builder.build()).build()
  }

}
