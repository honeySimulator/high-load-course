package ru.quipy.payments.logic

import ru.quipy.common.utils.*
import java.time.Duration
import java.util.*

interface PaymentService {
    /**
     * Submit payment request to external service.
     */
    fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long)
}

interface PaymentExternalService : PaymentService

/**
 * Describes properties of payment-provider accounts.
 */
data class ExternalServiceProperties(
    val serviceName: String,
    val accountName: String,
    val parallelRequests: Int,
    val rateLimitPerSec: Int,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11),
    val cost: Int
) {
    private val summary = Summary(request95thPercentileProcessingTime.toMillis().toDouble())

    /**
     * Requests per second
     */
    val speed: Double
        get() {
            val averageTime = summary.getAverageMillis()
            return if (averageTime != null) {
                minOf(parallelRequests / averageTime.toMillis() * 1000.0, rateLimitPerSec.toDouble())
            } else {
                // If the average request processing time is not available, we use a theoretical calculation
                minOf(parallelRequests * 1.0 / request95thPercentileProcessingTime.toMillis() * 1000, rateLimitPerSec * 1.0)
            }
        }
}

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)

/**
 * Describes request for external service.
 */
data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long
)