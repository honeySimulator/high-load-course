package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.CoroutineRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import java.time.Duration


@Configuration
class ExternalServicesConfig {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

//        private val accountProps_1 = ExternalServiceProperties(
//            serviceName = "test",
//            accountName = "default-1",
//            nonBlockingWindow = NonBlockingOngoingWindow(1000),
//            request95thPercentileProcessingTime = Duration.ofMillis(1000),
//            rateLimiter = CoroutineRateLimiter(100) // Используем CoroutineRateLimiter с ограничением в 100 запросов в секунду
//        )

        private val accountProps_2 = ExternalServiceProperties(
            serviceName = "test",
            accountName = "default-2",
            nonBlockingWindow = NonBlockingOngoingWindow(100),
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            rateLimiter = CoroutineRateLimiter(30) // Используем CoroutineRateLimiter с ограничением в 30 запросов в секунду
        )

        private val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            nonBlockingWindow = NonBlockingOngoingWindow(30),
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            rateLimiter = CoroutineRateLimiter(8)
        )

        // Call costs 30
        private val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            nonBlockingWindow = NonBlockingOngoingWindow(8),
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            rateLimiter = CoroutineRateLimiter(5)
        )
    }
    @Bean(PRIMARY_PAYMENT_BEAN)
    fun fastExternalService(): PaymentExternalServiceImpl {
        // Две конфигурации аккаунта
        return PaymentExternalServiceImpl(
            firstProperties = accountProps_4,
            secondProperties = accountProps_3,
            thirdProperties = accountProps_2
        )
    }
}