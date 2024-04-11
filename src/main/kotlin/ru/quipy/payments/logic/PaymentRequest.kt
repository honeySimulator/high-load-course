package ru.quipy.payments.logic

import java.util.*

data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long
)