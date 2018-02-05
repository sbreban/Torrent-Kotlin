package handlers

import com.google.protobuf.ByteString
import node.*
import util.ChunkInfoUtil

import java.security.NoSuchAlgorithmException
import java.util.ArrayList
import java.util.logging.Level
import java.util.logging.Logger

object LocalSearchRequestHandler {

  private val logger = Logger.getLogger(LocalSearchRequestHandler::class.java.name)

  fun handleLocalSearchRequest(message: Message, localFiles: MutableMap<ByteString, MutableList<ByteArray>>, fileNameToHash: MutableMap<String, ByteString>): Message? {
    var responseMessage: Message? = null
    try {
      val localSearchRequest = message.localSearchRequest
      val regex = localSearchRequest.regex
      val foundFileNames = fileNameToHash.keys.filter { it.matches(regex.toRegex()) }
      val fileInfos = ArrayList<FileInfo>()
      if (foundFileNames.isNotEmpty()) {
        for (foundFileName in foundFileNames) {
          val fileHash = fileNameToHash[foundFileName]
          val fileContent = localFiles[fileHash]

          var fileSize = 0
          if (fileContent != null) {
            for (bytes in fileContent) {
              fileSize += bytes.size
            }
          }

          val chunkInfos = ChunkInfoUtil.getChunkInfos(fileContent!!)
          logger.fine(chunkInfos.toString())

          val fileInfo = FileInfo.newBuilder().setHash(fileHash).setSize(fileSize).setFilename(foundFileName).addAllChunks(chunkInfos).build()
          fileInfos.add(fileInfo)
        }
      }

      val localSearchResponse = LocalSearchResponse.newBuilder().setStatus(Status.SUCCESS).addAllFileInfo(fileInfos).build()

      responseMessage = Message.newBuilder().setType(Message.Type.LOCAL_SEARCH_RESPONSE).setLocalSearchResponse(localSearchResponse).build()
    } catch (e: NoSuchAlgorithmException) {
      logger.log(Level.SEVERE, e.message)
    }

    return responseMessage
  }

}
