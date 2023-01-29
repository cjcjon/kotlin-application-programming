package chapter3.chapter4

import com.google.gson.Gson
import logger
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import java.text.SimpleDateFormat
import java.util.*

@RestController
@CrossOrigin(origins = ["*"], allowedHeaders = ["*"])
class ProducerController(val kafkaTemplate: KafkaTemplate<String, String>) {

  @GetMapping("/api/select")
  fun selectColor(
    @RequestHeader("user-agent") userAgentName: String,
    @RequestParam("color") colorName: String,
    @RequestParam("user") userName: String
  ) {
    val userEvent = UserEvent(SDF_DATE.format(Date()), userAgentName, colorName, userName)
    val jsonColorLog = Gson().toJson(userEvent)

    kafkaTemplate.send("select-color", jsonColorLog).addCallback(
      { result -> LOGGER.info(result?.toString()) },
      { ex -> LOGGER.error(ex.message, ex) }
    )
  }

  companion object {
    private val SDF_DATE = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
    private val LOGGER = logger()
  }
}