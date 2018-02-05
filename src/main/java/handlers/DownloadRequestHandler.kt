package handlers

import com.google.protobuf.ByteString
import node.DownloadResponse
import node.Message
import node.Status
import java.util.*
import java.util.logging.Logger

object DownloadRequestHandler {

  private val logger = Logger.getLogger(DownloadRequestHandler::class.java.name)

  fun handleDownloadRequest(message: Message, localFiles: MutableMap<ByteString, MutableList<ByteArray>>): Message {
    val builder = DownloadResponse.newBuilder()

    val downloadRequest = message.downloadRequest
    val fileHash = downloadRequest.fileHash
    if (fileHash.toByteArray().size != 16) {
      logger.severe("Invalid file hash")
      builder.status = Status.MESSAGE_ERROR
    } else {
      val dataList = LinkedList<ByteString>()

      val fileContent = localFiles[fileHash]
      if (fileContent != null && fileContent.isNotEmpty()) {
        for (i in fileContent.indices) {
          val content = fileContent[i]
          val data = ByteString.copyFrom(content)
          dataList.add(data)
        }
        builder.setStatus(Status.SUCCESS).data = ByteString.copyFrom(dataList)
      } else {
        builder.status = Status.UNABLE_TO_COMPLETE
      }
    }

    return Message.newBuilder().setType(Message.Type.DOWNLOAD_RESPONSE).setDownloadResponse(builder.build()).build()
  }

}
