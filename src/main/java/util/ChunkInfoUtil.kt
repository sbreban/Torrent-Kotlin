package util

import com.google.protobuf.ByteString
import node.ChunkInfo

import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.util.ArrayList
import java.util.Arrays
import java.util.logging.Logger

object ChunkInfoUtil {

  private val logger = Logger.getLogger(ChunkInfoUtil::class.java.name)

  @Throws(NoSuchAlgorithmException::class)
  fun getChunkInfos(bytes: ByteArray, fileContent: MutableList<ByteArray>): List<ChunkInfo> {
    val md = MessageDigest.getInstance("MD5")
    var digest: ByteArray
    val chunkInfos = ArrayList<ChunkInfo>()
    val blockSize = 1024
    val blockCount = (bytes.size + blockSize - 1) / blockSize

    var range: ByteArray? = null

    for (i in 0 until blockCount - 1) {
      val idx = i * blockSize
      range = Arrays.copyOfRange(bytes, idx, idx + blockSize)

      md.update(range!!)
      digest = md.digest()

      val chunkInfo = ChunkInfo.newBuilder().setIndex(i).setSize(blockSize).setHash(ByteString.copyFrom(digest)).build()
      fileContent.add(range)
      logger.fine("Chunk " + i + ": " + Arrays.toString(range))
      chunkInfos.add(chunkInfo)
    }

    val end: Int
    if (bytes.size % blockSize == 0) {
      end = bytes.size
    } else {
      end = bytes.size % blockSize + blockSize * (blockCount - 1)
    }
    range = Arrays.copyOfRange(bytes, (blockCount - 1) * blockSize, end)

    md.update(range!!)
    digest = md.digest()

    val chunkInfo = ChunkInfo.newBuilder().setIndex(blockCount - 1).setSize(end - (blockCount - 1) * blockSize).setHash(ByteString.copyFrom(digest)).build()
    fileContent.add(range)
    logger.fine("Chunk " + blockCount + ": " + Arrays.toString(range))
    chunkInfos.add(chunkInfo)
    return chunkInfos
  }

  @Throws(NoSuchAlgorithmException::class)
  fun getChunkInfos(fileContent: MutableList<ByteArray>): List<ChunkInfo> {
    val md = MessageDigest.getInstance("MD5")
    var digest: ByteArray
    val chunkInfos = ArrayList<ChunkInfo>()

    for (i in fileContent.indices) {
      val content = fileContent[i]
      md.update(content)
      digest = md.digest()

      val chunkInfo = ChunkInfo.newBuilder().setIndex(i).setSize(content.size).setHash(ByteString.copyFrom(digest)).build()
      logger.fine("Chunk " + chunkInfo.toString())
      chunkInfos.add(chunkInfo)
    }

    return chunkInfos
  }

}
