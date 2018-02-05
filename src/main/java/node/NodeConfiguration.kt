package node

import java.util.Objects

class NodeConfiguration(val addr: String, val port: Int) {

  override fun equals(o: Any?): Boolean {
    if (this === o) return true
    if (o == null || javaClass != o.javaClass) return false
    val that = o as NodeConfiguration?
    return port == that!!.port && addr == that.addr
  }

  override fun hashCode(): Int {

    return Objects.hash(addr, port)
  }
}
