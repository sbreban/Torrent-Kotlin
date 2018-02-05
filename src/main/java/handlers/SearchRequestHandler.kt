package handlers

import com.google.protobuf.ByteString
import node.*
import util.MessageUtil

import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import java.util.logging.Level
import java.util.logging.Logger

object SearchRequestHandler {

  private val logger = Logger.getLogger(SearchRequestHandler::class.java.name)

  fun handleSearchRequest(message: Message, localNode: NodeConfiguration, otherNodes: List<NodeConfiguration>, localFiles: MutableMap<ByteString, MutableList<ByteArray>>, fileNameToHash: MutableMap<String, ByteString>): Message {
    val builder = SearchResponse.newBuilder()

    try {
      val searchRequest = message.searchRequest
      val regex = searchRequest.regex

      val localSearchRequest = LocalSearchRequest.newBuilder().setRegex(regex).build()
      val localSearchRequestMessage = Message.newBuilder().setType(Message.Type.LOCAL_SEARCH_REQUEST).setLocalSearchRequest(localSearchRequest).build()

      for (nodeIndex in otherNodes.indices) {
        val otherNode = otherNodes[nodeIndex]

        val socket = Socket(InetAddress.getByName(otherNode.addr), otherNode.port)
        MessageUtil.sendMessage(socket, localSearchRequestMessage)
        val buffer = MessageUtil.getMessageBytes(socket)
        if (buffer != null) {
          val localSearchResponse = Message.parseFrom(buffer).localSearchResponse
          if (localSearchResponse.status == Status.SUCCESS) {
            addSearchResult(otherNode, builder, localSearchResponse)
          } else if (localSearchResponse.status == Status.UNABLE_TO_COMPLETE) {
            logger.fine("No search result from node " + otherNode)
          }
        }
      }

      val localSearchResponseMessage = LocalSearchRequestHandler.handleLocalSearchRequest(localSearchRequestMessage, localFiles, fileNameToHash)
      val localSearchResponse = localSearchResponseMessage!!.localSearchResponse
      if (localSearchResponse.status == Status.SUCCESS) {
        addSearchResult(localNode, builder, localSearchResponse)
      }

      builder.status = Status.SUCCESS
    } catch (e: IOException) {
      logger.log(Level.SEVERE, e.message)
      builder.status = Status.PROCESSING_ERROR
    }

    return Message.newBuilder().setType(Message.Type.SEARCH_RESPONSE).setSearchResponse(builder.build()).build()
  }

  private fun addSearchResult(nodeConfiguration: NodeConfiguration, builder: SearchResponse.Builder, localSearchResponse: LocalSearchResponse) {
    val node = Node.newBuilder().setPort(nodeConfiguration.port).setHost(nodeConfiguration.addr).build()
    val fileInfos = localSearchResponse.fileInfoList
    logger.fine("Found " + fileInfos.toString() + " on " + node)
    val nodeSearchResult = NodeSearchResult.newBuilder().setNode(node).setStatus(Status.SUCCESS).addAllFiles(fileInfos).build()
    builder.addResults(nodeSearchResult)
  }

}
