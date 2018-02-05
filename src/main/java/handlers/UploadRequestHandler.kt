package handlers

import com.google.protobuf.ByteString
import node.*
import util.ChunkInfoUtil

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.LinkedList
import java.util.logging.Level
import java.util.logging.Logger

object UploadRequestHandler {

  private val logger = Logger.getLogger(UploadRequestHandler::class.java.name)

  fun handleUploadRequest(message: Message, localFiles: MutableMap<ByteString, MutableList<ByteArray>>, fileNameToHash: MutableMap<String, ByteString>): Message {
    val builder = UploadResponse.newBuilder()

    val uploadRequest = message.uploadRequest
    val fileName = uploadRequest.filename

    if (fileName == null || fileName.isEmpty()) {
      logger.severe("Filename is empty")
      builder.status = Status.MESSAGE_ERROR
    } else if (fileNameToHash.containsKey(fileName)) {
      logger.fine("Already have the file")
      builder.status = Status.SUCCESS
    } else {
      try {

        val data = uploadRequest.data
        val bytes = data.toByteArray()

        val md = MessageDigest.getInstance("MD5")
        val digest: ByteArray
        md.update(bytes)
        digest = md.digest()
        val fileContent = LinkedList<ByteArray>()
        localFiles[ByteString.copyFrom(digest)] = fileContent
        fileNameToHash[fileName] = ByteString.copyFrom(digest)

        val chunkInfos = ChunkInfoUtil.getChunkInfos(bytes, fileContent)

        val fileInfo = FileInfo.newBuilder().setHash(ByteString.copyFrom(digest)).setSize(data.size()).setFilename(fileName).addAllChunks(chunkInfos).build()

        builder.status = Status.SUCCESS
        builder.fileInfo = fileInfo
      } catch (e: NoSuchAlgorithmException) {
        logger.log(Level.SEVERE, e.message)
      }

    }

    return Message.newBuilder().setType(Message.Type.UPLOAD_RESPONSE).setUploadResponse(builder.build()).build()
  }

}
