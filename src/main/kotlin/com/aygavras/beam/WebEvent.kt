package com.aygavras.beam

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer
import org.joda.time.DateTime
import java.io.Serializable

data class WebEvent(
        val eventTime: DateTime,
        val browser: String
) : Serializable

class WebEventSerializer : Serializer<WebEvent> {

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        print("configured")
    }

    override fun serialize(topic: String?, data: WebEvent?): ByteArray? {
        var retVal: ByteArray? = null
        val objectMapper = ObjectMapper()
        try {
            retVal = objectMapper.writeValueAsBytes(data)
        } catch (e: Exception) {
            e.printStackTrace()
        }
        return retVal

    }

    override fun close() {
        print("closed")
    }
}
