package ru.quipy.payments.logic

import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService

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
    /**
     * Requests per second
     */
    val speed get() =  minOf(parallelRequests * 1.0 / request95thPercentileProcessingTime.toMillis() * 1000, rateLimitPerSec * 1.0)
}

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)

val PaymentOperationTimeout = Duration.ofSeconds(80)