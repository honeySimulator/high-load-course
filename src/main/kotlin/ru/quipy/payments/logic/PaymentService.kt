package ru.quipy.payments.logic

import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
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
    val rateLimiter: CoroutineRateLimiter,
//    val blockingWindow: OngoingWindow,
    val nonBlockingWindow: NonBlockingOngoingWindow,
    val request95thPercentileProcessingTime: Duration = Duration.ofSeconds(11)
)

/**
 * Describes response from external service.
 */
class ExternalSysResponse(
    val result: Boolean,
    val message: String? = null,
)