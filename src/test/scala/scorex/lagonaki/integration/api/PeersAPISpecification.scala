package scorex.lagonaki.integration.api

import org.scalatest.{FunSuite, Matchers}
import scorex.lagonaki.integration.TestLock

class PeersAPISpecification extends FunSuite with TestLock with Matchers {

  import scorex.lagonaki.TestingCommons._

  test("/peers/connected API route") {
    val connected = GET.request("/peers/connected")
    (connected \\ "address").toList.size should be >= 1
    (connected \\ "declaredAddress").toList.size should be >= 1
    (connected \\ "peerName").toList.size should be >= 1
    (connected \\ "peerNonce").toList.size should be >= 1
  }

  test("/peers/all API route") {
    val all = GET.request("/peers/all")
    (all \\ "address").toList.size should be >= 1
    (all \\ "nodeName").toList.size should be >= 1
    (all \\ "nodeNonce").toList.size should be >= 1
    (all \\ "lastSeen").toList.size should be >= 1
  }

  test("/peers/blacklisted API route") {
    val blacklisted = GET.request("/peers/blacklisted")
    blacklisted.asOpt[Seq[String]].isDefined shouldBe true
  }

  test("/peers/connect API route") {
    POST.incorrectApiKeyTest("/peers/connect")

    val req = POST.request("/peers/connect", body = "{\"host\":\"127.0.0.1\",\"port\":123}")
    (req \ s"status").as[String] shouldBe "Trying to connect"
    (req \ "hostname").asOpt[String].isDefined shouldBe true
  }

}