package handlers

import com.google.protobuf.ByteString
import node.ChunkResponse
import node.Message
import node.Status
import java.util.logging.Logger

object ChunkRequestHandler {

  private val logger = Logger.getLogger(ChunkRequestHandler::class.java.name)

  fun handleChunkRequest(message: Message, localFiles: MutableMap<ByteString, MutableList<ByteArray>>): Message {
    val builder = ChunkResponse.newBuilder()

    val chunkRequest = message.chunkRequest
    val fileHash = chunkRequest.fileHash
    val chunkIndex = chunkRequest.chunkIndex

    if (fileHash.toByteArray().size != 16 || chunkIndex < 0) {
      logger.severe("Invalid file hash or chunk index")
      builder.status = Status.MESSAGE_ERROR
    } else {
      val fileContent = localFiles[fileHash]
      if (fileContent != null && fileContent.isNotEmpty()) {
        val chunk = fileContent[chunkIndex]
        builder.setStatus(Status.SUCCESS).data = ByteString.copyFrom(chunk)
        logger.fine("SUCCESS $fileHash $chunkIndex")
      } else {
        logger.fine("FAILURE $fileHash $chunkIndex")
        builder.status = Status.UNABLE_TO_COMPLETE
      }
    }

    return Message.newBuilder().setType(Message.Type.CHUNK_RESPONSE).setChunkResponse(builder.build()).build()
  }

}
