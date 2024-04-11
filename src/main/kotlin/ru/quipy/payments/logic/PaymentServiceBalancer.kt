package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.stereotype.Service
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.config.ServiceSet
import java.util.*

@Service
class PaymentServiceBalancer(
    serviceSets: List<ServiceSet>,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
    ) : PaymentExternalService, DisposableBean {

    private val queues = serviceSets.sortedBy { it.service.cost }.map{ x -> PaymentQueue(x) }.toTypedArray()
    private val logger = LoggerFactory.getLogger(PaymentServiceBalancer::class.java)

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val request = PaymentRequest(paymentId, amount, paymentStartedAt)
        queues.forEach {
            if (it.tryEnqueue(request)) {
                logger.warn("$paymentId can be placed in ${it.accountName}")
                return
            }
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request can't be processed due to lack of processing speed")
        }
    }

    override fun destroy() {
        logger.warn("Closing PaymentServiceBalancer")
        queues.forEach { it.destroy(); }
    }
}